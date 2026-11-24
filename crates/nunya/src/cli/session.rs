//! Owner-private session persistence and PostgreSQL command helpers.
use crate::{
    config::{load_for_management_client, parse_duration},
    mgmt::client::{
        AcquireParams, ClientError, ConnectInfo, Credentials, Endpoint, MgmtClient, SessionCredential,
        SessionSecret, SessionView,
    },
    state::model::{Mode, SessionState},
};
use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, OpenOptions},
    io::{BufRead, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};
use zeroize::Zeroizing;

const ENDED: &str = "the stored session has ended and its token scope was replaced; tokens from it will not match a new session. Run again with --new-scope to start a new scope.";
const DEFAULT_PG_HOST: &str = "127.0.0.1";
const DEFAULT_PG_PORT: u16 = 6432;
const UNVALIDATED: &str = "note: session not revalidated with the server (principal secret missing: pass --principal-secret-file or set NUNYA_PRINCIPAL_SECRET)";

/// Fixed, secret-free helper failures.
#[derive(Debug, PartialEq, Eq)]
pub enum HelperError {
    /// Stored scope cannot be resumed.
    ScopeReplaced,
    /// Rotation requires parameters from this helper's acquisition.
    RotationUnavailable,
    /// Storage unavailable or insecure.
    Store,
    /// Unsupported helper format.
    Version,
    /// Context does not exist.
    Missing,
    /// Context would be overwritten or used with another identity.
    Conflict,
    /// No management server was supplied.
    ServerMissing,
    /// Management destination was invalid.
    ServerInvalid,
    /// Bearer principal name was missing.
    PrincipalMissing,
    /// No bearer secret was supplied.
    PrincipalSecretMissing,
    /// The discovered config's bootstrap secret file is missing.
    OperatorSecretMissing(PathBuf),
    /// The discovered config's bootstrap secret file could not be used.
    OperatorSecret(String),
    /// Explicit secret file could not be read.
    PrincipalSecretFileUnreadable,
    /// Explicit secret file did not provide a secret.
    PrincipalSecretFileEmpty,
    /// Config discovery or validation failed.
    Config(PathBuf, String),
    /// The stored session cannot be used without revalidation.
    ExpiredWithoutSecret,
    /// An explicit exec option cannot be satisfied by the stored session.
    StoredOptionMismatch(&'static str),
    /// The command runtime could not start.
    Runtime,
    /// Management request refused or unavailable.
    Management(ClientError),
    /// Child could not be started.
    Child,
}
impl fmt::Display for HelperError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ScopeReplaced => ENDED,
            Self::RotationUnavailable => {
                "this session was not acquired by this helper; acquire a new session instead"
            }
            Self::Store => "session helper store unavailable or insecure",
            Self::Version => "unsupported session helper version",
            Self::Missing => "session context not found",
            Self::Conflict => "session context already exists or belongs to another connection",
            Self::ServerMissing => "server URL missing: pass --server, set NUNYA_SERVER, or run next to nunya.toml (--config / NUNYA_CONFIG)",
            Self::ServerInvalid => "invalid --server URL",
            Self::PrincipalMissing => "principal name missing: pass --principal or set NUNYA_PRINCIPAL",
            Self::PrincipalSecretMissing => "principal secret missing: pass --principal-secret-file PATH or set NUNYA_PRINCIPAL_SECRET",
            Self::OperatorSecretMissing(path) => {
                return write!(f, "principal secret missing: pass --principal-secret-file, set NUNYA_PRINCIPAL_SECRET, or create {} by running nunya serve once", path.display());
            }
            Self::OperatorSecret(error) => error,
            Self::PrincipalSecretFileUnreadable => "cannot read the principal secret file given by --principal-secret-file",
            Self::PrincipalSecretFileEmpty => "principal secret file is empty",
            Self::Config(path, error) => return write!(f, "config {}: {error}", path.display()),
            Self::ExpiredWithoutSecret => "stored session has expired locally and the principal secret is missing: pass --principal-secret-file PATH or set NUNYA_PRINCIPAL_SECRET",
            Self::StoredOptionMismatch(option) => {
                return write!(f, "cannot reuse stored session with {option}: the stored value differs or is unknown, and the principal secret is missing");
            }
            Self::Runtime => "session helper runtime unavailable",
            Self::Management(error) => {
                return write!(f, "session management request failed: {error}");
            }
            Self::Child => "unable to start child command",
        })
    }
}
impl HelperError {
    fn missing_secret(&self) -> bool {
        matches!(
            self,
            Self::PrincipalSecretMissing | Self::OperatorSecretMissing(_)
        )
    }
}
impl std::error::Error for HelperError {}
impl From<ClientError> for HelperError {
    fn from(error: ClientError) -> Self {
        Self::Management(error)
    }
}
mod secret_serde {
    use super::*;
    pub fn serialize<S: serde::Serializer>(v: &Zeroizing<String>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(v)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<Zeroizing<String>, D::Error> {
        String::deserialize(d).map(Zeroizing::new)
    }
}
/// Stored session connection and protected credential.
#[derive(Clone, Serialize, Deserialize)]
pub struct Context {
    /// Helper context name (the map key on disk).
    #[serde(skip)]
    pub name: String,
    /// Management destination.
    pub server: String,
    /// Principal name.
    pub principal: Option<String>,
    /// Route identifier.
    pub route: String,
    /// Configured route protocol kind, absent in legacy contexts.
    #[serde(default)]
    pub route_kind: Option<String>,
    /// Session identifier.
    pub session_id: String,
    /// Credential protected on drop.
    #[serde(with = "secret_serde")]
    pub secret: Zeroizing<String>,
    /// Standard base64 SCRAM salt retained from acquisition.
    #[serde(default)]
    pub scram_salt: Option<String>,
    /// SCRAM iteration count retained from acquisition.
    #[serde(default)]
    pub scram_iterations: Option<u32>,
    /// Scope identifier.
    pub scope: String,
    /// Expiration timestamp.
    pub expires_ms: u64,
    /// Last server-reported access mode, absent in legacy contexts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<Mode>,
    /// Issued session lifetime, absent in legacy contexts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifetime_ms: Option<u64>,
    /// PostgreSQL host persisted by older helpers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pg_host: Option<String>,
    /// PostgreSQL port persisted by older helpers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pg_port: Option<u16>,
    /// Explicit PostgreSQL host override.
    #[serde(default)]
    pub pg_host_override: Option<String>,
    /// Explicit PostgreSQL port override.
    #[serde(default)]
    pub pg_port_override: Option<u16>,
    /// Connection details most recently advertised by the server.
    #[serde(default)]
    pub connect: Option<ConnectInfo>,
    /// Scope isolation flag.
    pub isolated: bool,
    /// Optional label.
    pub label: Option<String>,
}
impl Context {
    fn refresh(&mut self, view: &SessionView) {
        self.expires_ms = view.expires_ms;
        self.mode = Some(view.mode);
        self.lifetime_ms = view.expires_ms.checked_sub(view.created_ms);
        self.route_kind.clone_from(&view.route_kind);
        self.connect.clone_from(&view.connect);
    }

    fn apply_pg_overrides(&mut self, opts: &AcquireOpts) {
        if let Some(host) = &opts.pg_host {
            self.pg_host_override = Some(host.clone());
        }
        if let Some(port) = opts.pg_port {
            self.pg_port_override = Some(port);
        }
    }

    fn ensure_unexpired(&self, now_ms: u128) -> Result<(), HelperError> {
        if u128::from(self.expires_ms) <= now_ms {
            return Err(HelperError::ExpiredWithoutSecret);
        }
        Ok(())
    }

    fn check_exec_options(&self, opts: &AcquireOpts) -> Result<(), HelperError> {
        if opts.route.as_ref().is_some_and(|route| route != &self.route) {
            return Err(HelperError::StoredOptionMismatch("--route"));
        }
        if opts.mode.is_some() && opts.mode != self.mode {
            return Err(HelperError::StoredOptionMismatch("--mode"));
        }
        if let Some(ttl) = &opts.ttl {
            let stored = self.lifetime_ms.map(std::time::Duration::from_millis);
            if stored.is_none() || parse_duration(ttl) != stored {
                return Err(HelperError::StoredOptionMismatch("--ttl"));
            }
        }
        if opts.label.is_some() && opts.label != self.label {
            return Err(HelperError::StoredOptionMismatch("--label"));
        }
        Ok(())
    }
}
impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Context { [REDACTED] }")
    }
}
#[derive(Serialize, Deserialize)]
struct Stored {
    version: u32,
    contexts: BTreeMap<String, Context>,
}
/// Per-user helper directory; Windows relies on the user profile ACL.
pub struct HelperStore {
    dir: PathBuf,
}
impl HelperStore {
    /// Select a specific directory, primarily for tests.
    pub fn at(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
    /// Locate the per-user helper directory.
    pub fn discover() -> Result<Self, HelperError> {
        if let Some(dir) = std::env::var_os("NUNYA_HOME") {
            return Ok(Self::at(dir));
        }
        #[cfg(unix)]
        let dir = std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")));
        #[cfg(windows)]
        let dir = std::env::var_os("LOCALAPPDATA").map(PathBuf::from);
        dir.map(|d| Self::at(d.join("nunya")))
            .ok_or(HelperError::Store)
    }
    fn secure(path: &Path, directory: bool) -> Result<(), HelperError> {
        let meta = fs::symlink_metadata(path).map_err(|_| HelperError::Store)?;
        if meta.file_type().is_symlink()
            || meta.is_dir() != directory
            || (!directory && !meta.is_file())
        {
            return Err(HelperError::Store);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o077 != 0 {
                return Err(HelperError::Store);
            }
        }
        // On Windows the per-user LOCALAPPDATA profile ACL protects helper files.
        Ok(())
    }
    fn prepare(&self) -> Result<(), HelperError> {
        if !self.dir.exists() {
            let mut builder = fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(&self.dir).map_err(|_| HelperError::Store)?;
        }
        Self::secure(&self.dir, true)
    }
    // File drop releases the advisory lock, including on every error path.
    fn lock(&self) -> Result<fs::File, HelperError> {
        self.prepare()?;
        let path = self.dir.join("sessions.lock");
        if path.try_exists().map_err(|_| HelperError::Store)? {
            Self::secure(&path, false)?;
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&path).map_err(|_| HelperError::Store)?;
        Self::secure(&path, false)?;
        file.lock().map_err(|_| HelperError::Store)?;
        Ok(file)
    }
    fn check_available(&self, name: &str) -> Result<(), HelperError> {
        if self.load()?.contexts.contains_key(name) {
            Err(HelperError::Conflict)
        } else {
            Ok(())
        }
    }
    fn put_new(&self, ctx: &Context) -> Result<(), HelperError> {
        let _lock = self.lock()?;
        self.check_available(&ctx.name)?;
        self.put_unlocked(ctx)
    }
    fn load(&self) -> Result<Stored, HelperError> {
        self.prepare()?;
        let path = self.dir.join("sessions.json");
        if !path.try_exists().map_err(|_| HelperError::Store)? {
            return Ok(Stored {
                version: 1,
                contexts: BTreeMap::new(),
            });
        }
        Self::secure(&path, false)?;
        let bytes = Zeroizing::new(fs::read(path).map_err(|_| HelperError::Store)?);
        #[derive(Deserialize)]
        struct Version {
            version: u32,
        }
        let value: Version = serde_json::from_slice(&bytes).map_err(|_| HelperError::Store)?;
        if value.version != 1 {
            return Err(HelperError::Version);
        }
        // Deserialize directly from the zeroizing bytes, avoiding another plaintext secret copy.
        serde_json::from_slice(&bytes).map_err(|_| HelperError::Store)
    }
    fn save(&self, data: &Stored) -> Result<(), HelperError> {
        self.prepare()?;
        let dest = self.dir.join("sessions.json");
        if dest.try_exists().map_err(|_| HelperError::Store)? {
            Self::secure(&dest, false)?;
        }
        let temp = self.dir.join(format!(
            ".sessions-{}.tmp",
            crate::token::SessionId::random().to_hex()
        ));
        let result = (|| {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temp).map_err(|_| HelperError::Store)?;
            Self::secure(&temp, false)?;
            let bytes = Zeroizing::new(serde_json::to_vec(data).map_err(|_| HelperError::Store)?);
            file.write_all(&bytes).map_err(|_| HelperError::Store)?;
            file.sync_all().map_err(|_| HelperError::Store)?;
            drop(file);
            fs::rename(&temp, &dest).map_err(|_| HelperError::Store)?;
            Self::secure(&dest, false)
        })();
        if result.is_err() {
            let _ = fs::remove_file(temp);
        }
        result
    }
    /// Read one context without exposing its secret in diagnostics.
    pub fn get(&self, name: &str) -> Result<Context, HelperError> {
        let mut ctx = self
            .load()?
            .contexts
            .remove(name)
            .ok_or(HelperError::Missing)?;
        ctx.name = name.into();
        Ok(ctx)
    }
    /// Save a context by its name.
    pub fn put(&self, ctx: &Context) -> Result<(), HelperError> {
        let _lock = self.lock()?;
        self.put_unlocked(ctx)
    }
    fn put_unlocked(&self, ctx: &Context) -> Result<(), HelperError> {
        let mut data = self.load()?;
        data.contexts.insert(ctx.name.clone(), ctx.clone());
        self.save(&data)
    }
    /// Delete a context.
    pub fn remove(&self, name: &str) -> Result<(), HelperError> {
        let _lock = self.lock()?;
        let mut data = self.load()?;
        data.contexts.remove(name);
        self.save(&data)
    }
}
/// Common management connection options.
#[derive(Args, Clone, Default)]
pub struct ConnectionOpts {
    /// Config file for connection defaults (defaults to ./nunya.toml if present).
    #[arg(long, env = "NUNYA_CONFIG")]
    pub config: Option<PathBuf>,
    /// Management URL.
    #[arg(long, env = "NUNYA_SERVER")]
    pub server: Option<String>,
    /// Follow one leader redirect (HTTPS endpoints and the same trusted CA required).
    #[arg(long)]
    pub follow_leader: bool,
    /// Trusted CA file.
    #[arg(long, env = "NUNYA_CA_FILE")]
    pub ca_file: Option<PathBuf>,
    /// Principal name.
    #[arg(long, env = "NUNYA_PRINCIPAL")]
    pub principal: Option<String>,
    /// Principal secret file (first line only).
    #[arg(long)]
    pub principal_secret_file: Option<PathBuf>,
    /// Stored context name.
    #[arg(long)]
    pub context: Option<String>,
}
struct ConnectionDefaults {
    server: String,
    operator_secret_file: PathBuf,
}
impl ConnectionDefaults {
    fn discover(config: Option<&Path>, local: &Path) -> Result<Option<Self>, HelperError> {
        let path = match config {
            Some(path) => path,
            None => match local.symlink_metadata() {
                Ok(_) => local,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(HelperError::Config(local.into(), error.to_string())),
            },
        };
        let config = load_for_management_client(path)
            .map_err(|errors| HelperError::Config(path.into(), errors.join("; ")))?;
        let management = config.management.ok_or(HelperError::ServerMissing)?;
        Ok(Some(Self {
            server: management_url(management.listen, management.tls.is_some()),
            operator_secret_file: config.state_dir.join("operator.secret"),
        }))
    }
}

fn management_url(listen: SocketAddr, tls: bool) -> String {
    let ip = match listen.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => Ipv4Addr::LOCALHOST.into(),
        IpAddr::V6(ip) if ip.is_unspecified() => Ipv6Addr::LOCALHOST.into(),
        ip => ip,
    };
    let scheme = if tls { "https" } else { "http" };
    format!("{scheme}://{}", SocketAddr::new(ip, listen.port()))
}

struct ResolvedConnection {
    server: String,
    principal: Option<String>,
    client: MgmtClient,
}

impl ConnectionOpts {
    fn name(&self) -> &str {
        self.context.as_deref().unwrap_or("default")
    }
    pub(super) fn client(&self) -> Result<MgmtClient, HelperError> {
        self.client_with_context(None)
    }

    fn client_with_context(&self, context: Option<&Context>) -> Result<MgmtClient, HelperError> {
        Ok(self.connect(context)?.client)
    }

    fn connect(&self, context: Option<&Context>) -> Result<ResolvedConnection, HelperError> {
        self.connect_with(context, Path::new("nunya.toml"), || {
            std::env::var("NUNYA_PRINCIPAL_SECRET").ok()
        })
    }

    fn connect_with(
        &self,
        context: Option<&Context>,
        local_config: &Path,
        secret_env: impl FnOnce() -> Option<String>,
    ) -> Result<ResolvedConnection, HelperError> {
        let server = self
            .server
            .as_deref()
            .or_else(|| context.map(|ctx| ctx.server.as_str()));
        let defaults = if server.is_none() {
            ConnectionDefaults::discover(self.config.as_deref(), local_config)?
        } else {
            None
        };
        let server = server
            .or_else(|| defaults.as_ref().map(|config| config.server.as_str()))
            .ok_or(HelperError::ServerMissing)?
            .to_owned();
        let endpoint: Endpoint = server.parse().map_err(|_| HelperError::ServerInvalid)?;
        let mut principal = self
            .principal
            .clone()
            .or_else(|| context.and_then(|ctx| ctx.principal.clone()));
        let credentials = if server.starts_with("unix:") {
            Credentials::None
        } else {
            let mut secret = match &self.principal_secret_file {
                Some(path) => Some(read_principal_secret(path)?),
                None => secret_env()
                    .map(Zeroizing::new)
                    .filter(|text| !text.is_empty()),
            };
            if let Some(config) = &defaults
                && (principal.is_none()
                    || (principal.as_deref() == Some("operator") && secret.is_none()))
            {
                let path = &config.operator_secret_file;
                let exists = path.try_exists().map_err(|error| {
                    HelperError::OperatorSecret(format!(
                        "cannot read administrator secret at {}: {error}",
                        path.display()
                    ))
                })?;
                if exists {
                    if secret.is_none() {
                        secret = Some(crate::first_run::read_operator_secret(path)
                            .map_err(HelperError::OperatorSecret)?);
                    }
                    principal.get_or_insert_with(|| "operator".into());
                } else if secret.is_none() {
                    return Err(HelperError::OperatorSecretMissing(path.clone()));
                }
            }
            let name = principal
                .as_ref()
                .filter(|name| !name.is_empty())
                .ok_or(HelperError::PrincipalMissing)?
                .clone();
            let secret = secret.ok_or(HelperError::PrincipalSecretMissing)?;
            Credentials::Bearer { name, secret }
        };
        let client = MgmtClient::new(endpoint, self.ca_file.as_deref(), credentials)?
            .follow_leader_redirect(self.follow_leader);
        Ok(ResolvedConnection {
            server,
            principal,
            client,
        })
    }

    fn revalidation_client(&self, ctx: &Context) -> Result<Option<MgmtClient>, HelperError> {
        match self.client_with_context(Some(ctx)) {
            Ok(client) => Ok(Some(client)),
            Err(error) if error.missing_secret() => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn context(&self, store: &HelperStore) -> Result<Context, HelperError> {
        let ctx = store.get(self.name())?;
        if self
            .server
            .as_ref()
            .is_some_and(|server| *server != ctx.server)
            || self
                .principal
                .as_ref()
                .is_some_and(|principal| Some(principal) != ctx.principal.as_ref())
        {
            return Err(HelperError::Conflict);
        }
        Ok(ctx)
    }
}

fn read_principal_secret(path: &Path) -> Result<Zeroizing<String>, HelperError> {
    let file = fs::File::open(path).map_err(|_| HelperError::PrincipalSecretFileUnreadable)?;
    let mut text = Zeroizing::new(String::new());
    std::io::BufReader::new(file)
        .read_line(&mut text)
        .map_err(|_| HelperError::PrincipalSecretFileUnreadable)?;
    while text.ends_with(['\r', '\n']) {
        text.pop();
    }
    if text.is_empty() {
        return Err(HelperError::PrincipalSecretFileEmpty);
    }
    Ok(text)
}
fn mode(text: &str) -> Result<Mode, String> {
    match text {
        "read-only" => Ok(Mode::ReadOnly),
        "read-write" => Ok(Mode::ReadWrite),
        _ => Err("invalid session mode".into()),
    }
}
fn ttl(text: &str) -> Result<String, String> {
    parse_duration(text)
        .map(|_| text.to_owned())
        .ok_or_else(|| "invalid session duration".into())
}
/// Shared acquisition flags (isolation is restricted to session acquire).
#[derive(Args, Clone)]
pub struct AcquireOpts {
    /// Connection and context options.
    #[command(flatten)]
    pub connection: ConnectionOpts,
    /// Route identifier (defaults to postgres for a new session).
    #[arg(long)]
    pub route: Option<String>,
    /// Requested mode.
    #[arg(long, value_parser = mode)]
    pub mode: Option<Mode>,
    /// Requested lifetime.
    #[arg(long, value_parser = ttl)]
    pub ttl: Option<String>,
    /// Session label.
    #[arg(long)]
    pub label: Option<String>,
    /// Explicit permission to replace an ended scope.
    #[arg(long)]
    pub new_scope: bool,
    /// Rotate the credential of an existing session.
    #[arg(long, conflicts_with = "new_scope")]
    pub rotate: bool,
    /// Override the advertised PostgreSQL host.
    #[arg(long, env = "NUNYA_PG_HOST")]
    pub pg_host: Option<String>,
    /// Override the advertised PostgreSQL port.
    #[arg(long, env = "NUNYA_PG_PORT")]
    pub pg_port: Option<u16>,
    /// Internal isolation flag; exposed only by session acquire.
    #[arg(skip)]
    pub isolated: bool,
}
/// Session lifecycle commands.
#[derive(Subcommand)]
pub enum SessionCommand {
    /// Acquire or resume a stored session.
    Acquire {
        /// Acquisition and connection options.
        #[command(flatten)]
        opts: AcquireOpts,
        /// Create a separate scope in a new context.
        #[arg(long, conflicts_with = "rotate")]
        isolated: bool,
    },
    /// Display public session details.
    Show(ConnectionOpts),
    /// Renew the stored session.
    Renew {
        /// Connection and context options.
        #[command(flatten)]
        connection: ConnectionOpts,
        /// Requested access mode.
        #[arg(long, value_parser = mode)]
        mode: Option<Mode>,
        /// Rotate the stored credential.
        #[arg(long)]
        rotate: bool,
    },
    /// Revoke and remove the stored session.
    Revoke(ConnectionOpts),
    /// Print shell environment credentials.
    Env {
        /// Connection and context options.
        #[command(flatten)]
        connection: ConnectionOpts,
        /// Shell output syntax.
        #[arg(long, value_enum, default_value = "sh")]
        format: EnvFormat,
    },
    /// Acquire or resume a session and run a child command.
    Exec(ExecOpts),
}
/// Child process options.
#[derive(Args)]
pub struct ExecOpts {
    /// Acquisition options.
    #[command(flatten)]
    opts: AcquireOpts,
    /// Command and arguments after --.
    #[arg(last = true, required = true)]
    command: Vec<std::ffi::OsString>,
}
/// Environment output syntax.
#[derive(Clone, Copy, ValueEnum)]
pub enum EnvFormat {
    /// POSIX shell.
    Sh,
    /// PowerShell.
    Powershell,
}
fn ended(error: &ClientError) -> bool {
    matches!(error, ClientError::Api { status: 404, .. })
        || matches!(error, ClientError::Api { status: 409, code } if code == "session_ended")
}
async fn resume(client: &MgmtClient, ctx: &mut Context) -> Result<SessionView, HelperError> {
    let view = client.session(&ctx.session_id).await.map_err(|e| {
        if ended(&e) {
            HelperError::ScopeReplaced
        } else {
            HelperError::Management(e)
        }
    })?;
    if view.state != SessionState::Active || view.scope != ctx.scope {
        return Err(HelperError::ScopeReplaced);
    }
    ctx.refresh(&view);
    // Renewal authoritatively checks expiration using the server's clock.
    let view = client
        .renew(&ctx.session_id, None, None)
        .await
        .map_err(|e| {
            if ended(&e) {
                HelperError::ScopeReplaced
            } else {
                HelperError::Management(e)
            }
        })?;
    ctx.refresh(&view);
    Ok(view)
}
/// Acquire a new session or authoritatively revalidate a stored session.
pub async fn acquire_or_resume(
    client: &MgmtClient,
    store: &HelperStore,
    opts: &AcquireOpts,
) -> Result<Context, HelperError> {
    let server = opts
        .connection
        .server
        .as_ref()
        .ok_or(HelperError::ServerMissing)?;
    let name = opts.connection.name();
    if opts.rotate {
        if opts.new_scope || opts.isolated {
            return Err(HelperError::Conflict);
        }
        let mut ctx = opts.connection.context(store)?;
        ctx.apply_pg_overrides(opts);
        rotate(client, store, &mut ctx, opts.mode).await?;
        return Ok(ctx);
    }
    let existing = {
        let _lock = store.lock()?;
        let existing = store.load()?.contexts.contains_key(name);
        if opts.isolated && opts.connection.context.is_some() {
            store.check_available(name)?;
        }
        existing
    };
    if opts.isolated {
        if opts.connection.context.is_some() && existing {
            return Err(HelperError::Conflict);
        }
    } else if existing {
        let mut ctx = opts.connection.context(store)?;
        ctx.apply_pg_overrides(opts);
        match resume(client, &mut ctx).await {
            Ok(_) => {
                store.put(&ctx)?;
                return Ok(ctx);
            }
            Err(HelperError::ScopeReplaced) if opts.new_scope => store.remove(name)?,
            Err(e) => return Err(e),
        }
    }
    let secret = SessionSecret::generate();
    let view = client
        .acquire(AcquireParams {
            route: opts.route.clone().unwrap_or_else(|| "postgres".into()),
            mode: opts.mode,
            lifetime: opts.ttl.clone(),
            isolated: opts.isolated,
            label: opts.label.clone(),
            verifier: secret.verifier,
            secret_sha256: secret.sha256_hex,
        })
        .await?;
    let acquired_id = view.id.clone();
    let result = (|| {
        let name = if opts.isolated && opts.connection.context.is_none() {
            format!(
                "isolated-{}",
                view.id
                    .get(..8)
                    .ok_or(HelperError::Management(ClientError::Decode))?
            )
        } else {
            name.into()
        };
        let ctx = Context {
            name,
            server: server.clone(),
            principal: opts.connection.principal.clone(),
            route: view.route,
            route_kind: view.route_kind,
            session_id: view.id,
            secret: secret.secret,
            scram_salt: Some(secret.scram_salt),
            scram_iterations: Some(secret.scram_iterations),
            scope: view.scope,
            expires_ms: view.expires_ms,
            mode: Some(view.mode),
            lifetime_ms: view.expires_ms.checked_sub(view.created_ms),
            pg_host: None,
            pg_port: None,
            pg_host_override: opts.pg_host.clone(),
            pg_port_override: opts.pg_port,
            connect: view.connect,
            isolated: view.isolated,
            label: view.label,
        };
        store.put_new(&ctx)?;
        Ok(ctx)
    })();
    if result.is_err() {
        let _ = client.revoke(&acquired_id).await;
    }
    result
}
fn rotation_parameters(ctx: &Context) -> Result<(Vec<u8>, u32), HelperError> {
    let salt = ctx
        .scram_salt
        .as_deref()
        .ok_or(HelperError::RotationUnavailable)?;
    let iterations = ctx
        .scram_iterations
        .ok_or(HelperError::RotationUnavailable)?;
    let salt = data_encoding::BASE64
        .decode(salt.as_bytes())
        .map_err(|_| ClientError::Decode)?;
    Ok((salt, iterations))
}
/// Rotate a stored session credential while preserving its session and scope.
pub async fn rotate(
    client: &MgmtClient,
    store: &HelperStore,
    ctx: &mut Context,
    mode: Option<Mode>,
) -> Result<(), HelperError> {
    let (salt, iterations) = rotation_parameters(ctx)?;
    let current = client.session(&ctx.session_id).await?;
    if current.state != SessionState::Active || current.scope != ctx.scope {
        return Err(HelperError::ScopeReplaced);
    }
    ctx.refresh(&current);
    let secret = SessionSecret::generate_with(&salt, iterations)?;
    let view = client
        .renew(
            &ctx.session_id,
            mode,
            Some(SessionCredential {
                verifier: secret.verifier,
                secret_sha256: secret.sha256_hex,
            }),
        )
        .await?;
    let mut updated = ctx.clone();
    updated.secret = secret.secret;
    updated.refresh(&view);
    store.put(&updated)?;
    *ctx = updated;
    println!("The previous secret keeps working until the session expires.");
    Ok(())
}
/// Connection environment values, including protected session credentials.
pub fn pg_env(ctx: &Context) -> Vec<(String, Zeroizing<String>)> {
    let native = match &ctx.connect {
        Some(details) => matches!(details, ConnectInfo::SpannerNative { .. }),
        None => ctx
            .route_kind
            .as_deref()
            .map_or(ctx.route == "spanner-native", |kind| {
                kind == "spanner-native"
            }),
    };
    if native {
        let mut bearer = Zeroizing::new(String::from("nunya.session:"));
        bearer.push_str(&ctx.session_id);
        bearer.push('.');
        bearer.push_str(&ctx.secret);
        let mut env = vec![("NUNYA_SPANNER_BEARER".into(), bearer)];
        if let Some(ConnectInfo::SpannerNative { endpoint, database }) = &ctx.connect {
            env.push((
                "NUNYA_SPANNER_ENDPOINT".into(),
                Zeroizing::new(endpoint.clone()),
            ));
            env.push((
                "NUNYA_SPANNER_DATABASE".into(),
                Zeroizing::new(database.clone()),
            ));
        }
        return env;
    }

    let (host, port, database) = match &ctx.connect {
        Some(ConnectInfo::Postgres {
            host,
            port,
            database,
        }) => (host.as_str(), *port, Some(database)),
        _ => (DEFAULT_PG_HOST, DEFAULT_PG_PORT, None),
    };
    // A legacy value that differs from the old default came from a flag or env.
    let host = ctx
        .pg_host_override
        .as_deref()
        .or_else(|| {
            ctx.pg_host
                .as_deref()
                .filter(|value| *value != DEFAULT_PG_HOST)
        })
        .unwrap_or(host);
    let port = ctx
        .pg_port_override
        .or_else(|| ctx.pg_port.filter(|value| *value != DEFAULT_PG_PORT))
        .unwrap_or(port);
    let mut env = vec![
        ("PGHOST".into(), Zeroizing::new(host.to_owned())),
        ("PGPORT".into(), Zeroizing::new(port.to_string())),
        (
            "PGUSER".into(),
            Zeroizing::new(format!("session:{}", ctx.session_id)),
        ),
        ("PGPASSWORD".into(), ctx.secret.clone()),
    ];
    if let Some(database) = database {
        env.push(("PGDATABASE".into(), Zeroizing::new(database.clone())));
    }
    env
}
fn quoted(value: &str, format: EnvFormat) -> Zeroizing<String> {
    Zeroizing::new(match format {
        EnvFormat::Sh => format!("'{}'", value.replace('\'', "'\\''")),
        EnvFormat::Powershell => format!("'{}'", value.replace('\'', "''")),
    })
}
fn print_session(ctx: &Context, view: Option<&SessionView>) {
    println!("id: {}\nroute: {}", ctx.session_id, ctx.route);
    if let Some(view) = view {
        println!("mode: {:?}", view.mode);
    }
    println!(
        "expires_ms: {}\ncontext: {}\nconnect: {}",
        ctx.expires_ms,
        ctx.name,
        serde_json::json!(ctx.connect)
    );
}

async fn summary(client: &MgmtClient, store: &HelperStore, ctx: &mut Context) -> Result<(), HelperError> {
    let view = client.session(&ctx.session_id).await?;
    ctx.refresh(&view);
    store.put(ctx)?;
    print_session(ctx, Some(&view));
    Ok(())
}
/// Revoke a session and delete its stored context after success.
pub async fn revoke(
    client: &MgmtClient,
    store: &HelperStore,
    ctx: &Context,
) -> Result<(), HelperError> {
    client.revoke(&ctx.session_id).await?;
    store.remove(&ctx.name)
}
async fn dispatch(command: SessionCommand) -> Result<i32, HelperError> {
    let store = HelperStore::discover()?;
    match command {
        SessionCommand::Acquire { mut opts, isolated } => {
            opts.isolated = isolated;
            let existing = acquisition_context(&opts, &store)?;
            let client = connect_acquisition(&mut opts, existing.as_ref())?;
            let mut ctx = acquire_or_resume(&client, &store, &opts).await?;
            summary(&client, &store, &mut ctx).await?;
        }
        SessionCommand::Show(connection) => {
            let mut ctx = connection.context(&store)?;
            if let Some(client) = connection.revalidation_client(&ctx)? {
                let view = client.session(&ctx.session_id).await?;
                ctx.refresh(&view);
                store.put(&ctx)?;
                print_session(&ctx, Some(&view));
                println!(
                    "state: {:?}\nrevoke_reason: {:?}\nscope_continuity: {}",
                    view.state, view.revoke_reason, view.scope_continuity
                );
            } else {
                print_session(&ctx, None);
                eprintln!("{UNVALIDATED}");
            }
        }
        SessionCommand::Renew {
            connection,
            mode,
            rotate: rotating,
        } => {
            let mut ctx = connection.context(&store)?;
            let client = connection.client_with_context(Some(&ctx))?;
            if rotating {
                rotate(&client, &store, &mut ctx, mode).await?;
            } else {
                let view = client.renew(&ctx.session_id, mode, None).await?;
                ctx.refresh(&view);
                store.put(&ctx)?;
            }
            summary(&client, &store, &mut ctx).await?;
        }
        SessionCommand::Revoke(connection) => {
            let ctx = connection.context(&store)?;
            let client = connection.client_with_context(Some(&ctx))?;
            revoke(&client, &store, &ctx).await?;
            println!("session revoked");
        }
        SessionCommand::Env { connection, format } => {
            let mut ctx = connection.context(&store)?;
            if let Some(client) = connection.revalidation_client(&ctx)? {
                resume(&client, &mut ctx).await?;
                store.put(&ctx)?;
            } else {
                ctx.ensure_unexpired(
                    UNIX_EPOCH
                        .elapsed()
                        .map_or(u128::MAX, |now| now.as_millis()),
                )?;
                eprintln!("{UNVALIDATED}");
            }
            for (key, value) in pg_env(&ctx) {
                let value = quoted(&value, format);
                match format {
                    EnvFormat::Sh => println!("export {key}={}", value.as_str()),
                    EnvFormat::Powershell => println!("$env:{key} = {}", value.as_str()),
                }
            }
        }
        SessionCommand::Exec(opts) => return exec(opts, &store).await,
    }
    Ok(0)
}
fn runtime(result: impl std::future::Future<Output = Result<i32, HelperError>>) -> i32 {
    let result = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt.block_on(result),
        Err(_) => Err(HelperError::Runtime),
    };
    match result {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            if error == HelperError::ScopeReplaced {
                2
            } else {
                1
            }
        }
    }
}
/// Run a session command in a dedicated runtime.
pub fn run_command(command: SessionCommand) -> i32 {
    runtime(dispatch(command))
}

fn acquisition_context(
    opts: &AcquireOpts,
    store: &HelperStore,
) -> Result<Option<Context>, HelperError> {
    if opts.isolated {
        return Ok(None);
    }
    match opts.connection.context(store) {
        Ok(ctx) => Ok(Some(ctx)),
        Err(HelperError::Missing) if !opts.rotate => Ok(None),
        Err(error) => Err(error),
    }
}

fn connect_acquisition(
    opts: &mut AcquireOpts,
    ctx: Option<&Context>,
) -> Result<MgmtClient, HelperError> {
    let resolved = opts.connection.connect(ctx)?;
    opts.connection.server = Some(resolved.server);
    opts.connection.principal = resolved.principal;
    Ok(resolved.client)
}

async fn exec(mut opts: ExecOpts, store: &HelperStore) -> Result<i32, HelperError> {
    let existing = acquisition_context(&opts.opts, store)?;
    let ctx = match connect_acquisition(&mut opts.opts, existing.as_ref()) {
        Ok(client) => acquire_or_resume(&client, store, &opts.opts).await?,
        Err(error) if error.missing_secret() && !opts.opts.rotate && !opts.opts.new_scope => {
            let mut ctx = existing.ok_or(error)?;
            ctx.ensure_unexpired(UNIX_EPOCH.elapsed().map_or(u128::MAX, |now| now.as_millis()))?;
            ctx.check_exec_options(&opts.opts)?;
            ctx.apply_pg_overrides(&opts.opts);
            eprintln!("{UNVALIDATED}");
            ctx
        }
        Err(error) => return Err(error),
    };
    let program = opts.command.first().ok_or(HelperError::Child)?;
    let mut child = std::process::Command::new(program);
    child.args(&opts.command[1..]);
    for (key, value) in pg_env(&ctx) {
        child.env(key, value.as_str());
    }
    let status = child.status().map_err(|_| HelperError::Child)?;
    Ok(status.code().unwrap_or(1))
}

/// Acquire a session and execute a child with inherited standard streams.
pub fn run_exec(opts: ExecOpts) -> i32 {
    runtime(dispatch(SessionCommand::Exec(opts)))
}
#[cfg(test)]
mod tests {
    use super::*;
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "ny-{}",
                &crate::token::SessionId::random().to_hex()[..8]
            )))
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn context() -> Context {
        Context {
            name: "default".into(),
            server: "unix:/tmp/nunya.sock".into(),
            principal: None,
            route: "postgres".into(),
            route_kind: None,
            session_id: "12".repeat(16),
            secret: Zeroizing::new("private-session-secret".into()),
            scram_salt: None,
            scram_iterations: None,
            scope: "34".repeat(16),
            expires_ms: 12345,
            mode: None,
            lifetime_ms: None,
            pg_host: None,
            pg_port: None,
            pg_host_override: None,
            pg_port_override: None,
            connect: None,
            isolated: false,
            label: None,
        }
    }
    #[test]
    fn spanner_environment_has_protected_session_bearer() {
        let mut ctx = context();
        ctx.route = "native-renamed".into();
        ctx.route_kind = Some("spanner-native".into());
        ctx.connect = Some(ConnectInfo::SpannerNative {
            endpoint: "spanner.example:7443".into(),
            database: "projects/p/instances/i/databases/d".into(),
        });
        ctx.pg_port_override = Some(4444);
        let env = pg_env(&ctx);
        let bearer = &env
            .iter()
            .find(|(key, _)| key == "NUNYA_SPANNER_BEARER")
            .unwrap()
            .1;
        let mut header = Zeroizing::new(String::from("Bearer "));
        header.push_str(bearer);
        let (user, secret) = crate::spanner::auth::parse_authorization(&header).unwrap();
        assert_eq!(
            crate::pgwire::auth::parse_session_user(user)
                .unwrap()
                .to_hex(),
            ctx.session_id
        );
        assert!(secret == ctx.secret.as_str(), "session credential mismatch");
        assert!(env.iter().all(|(key, _)| !key.starts_with("PG")));
        let env = BTreeMap::from_iter(env);
        assert_eq!(
            env["NUNYA_SPANNER_ENDPOINT"].as_str(),
            "spanner.example:7443"
        );
        assert_eq!(
            env["NUNYA_SPANNER_DATABASE"].as_str(),
            "projects/p/instances/i/databases/d"
        );
        assert!(!format!("{ctx:?}").contains(ctx.secret.as_str()));
    }
    #[test]
    fn spanner_bearer_respects_kind_and_legacy_fallback() {
        let mut ctx = context();
        ctx.route = "spanner-native".into();
        assert!(pg_env(&ctx).iter().any(|(key, _)| key == "NUNYA_SPANNER_BEARER"));
        ctx.route_kind = Some("postgres".into());
        assert!(!pg_env(&ctx).iter().any(|(key, _)| key == "NUNYA_SPANNER_BEARER"));
        let mut legacy = serde_json::to_value(&ctx).unwrap();
        legacy.as_object_mut().unwrap().remove("route_kind");
        let legacy: Context = serde_json::from_value(legacy).unwrap();
        assert!(legacy.route_kind.is_none());
        assert!(pg_env(&legacy).iter().any(|(key, _)| key == "NUNYA_SPANNER_BEARER"));
    }
    #[test]
    fn session_view_accepts_route_kind_and_legacy_response() {
        let mut value = serde_json::json!({
            "id": "id", "principal": "principal", "route": "native-renamed",
            "mode": "read_only", "state": "active", "created_ms": 0,
            "expires_ms": 1, "last_used_ms": 0, "isolated": false,
            "scope": "scope", "scope_continuity": true
        });
        let legacy: SessionView = serde_json::from_value(value.clone()).unwrap();
        assert!(legacy.route_kind.is_none());
        assert_eq!(legacy.connect, None);
        value["route_kind"] = "spanner-native".into();
        value["connect"] = serde_json::json!({
            "kind": "spanner-native",
            "endpoint": "[::1]:7443",
            "database": "projects/p/instances/i/databases/d",
        });
        let current: SessionView = serde_json::from_value(value).unwrap();
        let mut ctx = context();
        ctx.refresh(&current);
        assert_eq!(ctx.route_kind.as_deref(), Some("spanner-native"));
        assert_eq!(ctx.connect, current.connect);
        assert_eq!(ctx.expires_ms, current.expires_ms);
    }
    #[test]
    fn postgres_environment_uses_advertised_values_and_explicit_overrides() {
        let mut ctx = context();
        let env = BTreeMap::from_iter(pg_env(&ctx));
        assert_eq!(env.len(), 4);
        assert_eq!(env["PGHOST"].as_str(), "127.0.0.1");
        assert_eq!(env["PGPORT"].as_str(), "6432");
        ctx.connect = Some(ConnectInfo::Postgres {
            host: "pg.example".into(),
            port: 7443,
            database: "app".into(),
        });
        let env = BTreeMap::from_iter(pg_env(&ctx));
        assert_eq!(env.len(), 5);
        assert_eq!(env["PGHOST"].as_str(), "pg.example");
        assert_eq!(env["PGPORT"].as_str(), "7443");
        assert_eq!(env["PGDATABASE"].as_str(), "app");
        assert_eq!(
            env["PGUSER"].as_str(),
            format!("session:{}", ctx.session_id)
        );
        assert!(env["PGPASSWORD"].as_str() == ctx.secret.as_str());

        // Even an explicitly requested old default outranks the server.
        ctx.pg_host_override = Some("127.0.0.1".into());
        ctx.pg_port_override = Some(6432);
        let env = BTreeMap::from_iter(pg_env(&ctx));
        assert_eq!(env["PGHOST"].as_str(), "127.0.0.1");
        assert_eq!(env["PGPORT"].as_str(), "6432");
        assert_eq!(env["PGDATABASE"].as_str(), "app");
    }
    #[test]
    fn legacy_context_retains_custom_pg_values_and_accepts_advertised_defaults() {
        let mut old = serde_json::to_value(context()).unwrap();
        let object = old.as_object_mut().unwrap();
        for key in ["connect", "pg_host_override", "pg_port_override"] {
            object.remove(key);
        }
        old["pg_host"] = "127.0.0.1".into();
        old["pg_port"] = 7432.into();
        let mut ctx: Context = serde_json::from_value(old).unwrap();
        assert_eq!(ctx.connect, None);
        let env = BTreeMap::from_iter(pg_env(&ctx));
        assert_eq!(env["PGHOST"].as_str(), "127.0.0.1");
        assert_eq!(env["PGPORT"].as_str(), "7432");
        ctx.connect = Some(ConnectInfo::Postgres {
            host: "pg.example".into(),
            port: 7443,
            database: "app".into(),
        });
        let env = BTreeMap::from_iter(pg_env(&ctx));
        assert_eq!(env["PGHOST"].as_str(), "pg.example");
        assert_eq!(env["PGPORT"].as_str(), "7432");
    }
    #[test]
    fn stored_connection_fallback_allows_only_the_same_server_and_principal() {
        let temp = Temp::new();
        let store = HelperStore::at(&temp.0);
        let mut ctx = context();
        ctx.server = "http://127.0.0.1:9876".into();
        ctx.principal = Some("reader".into());
        store.put(&ctx).unwrap();
        let secret_file = temp.0.join("principal.secret");
        fs::write(&secret_file, "private-principal-secret\r\n").unwrap();
        let mut opts = ConnectionOpts {
            principal_secret_file: Some(secret_file),
            ..Default::default()
        };
        assert!(
            opts.client_with_context(Some(&opts.context(&store).unwrap()))
                .is_ok()
        );
        opts.server = Some("http://127.0.0.1:7890".into());
        assert_eq!(opts.context(&store).unwrap_err(), HelperError::Conflict);
        opts.server = None;
        opts.principal = Some("other".into());
        assert_eq!(opts.context(&store).unwrap_err(), HelperError::Conflict);
        assert_eq!(ctx.ensure_unexpired(12344), Ok(()));
        assert_eq!(
            ctx.ensure_unexpired(12345),
            Err(HelperError::ExpiredWithoutSecret)
        );
    }
    #[test]
    fn connection_errors_describe_what_is_missing_without_secrets() {
        let temp = Temp::new();
        fs::create_dir(&temp.0).unwrap();
        let secret_file = temp.0.join("principal.secret");
        let mut opts = ConnectionOpts {
            principal_secret_file: Some(secret_file.clone()),
            ..Default::default()
        };
        let local_config = temp.0.join("nunya.toml");
        let error = |opts: &ConnectionOpts| {
            opts.connect_with(None, &local_config, || None)
                .err()
                .unwrap()
                .to_string()
        };
        assert_eq!(
            error(&opts),
            "server URL missing: pass --server, set NUNYA_SERVER, or run next to nunya.toml (--config / NUNYA_CONFIG)"
        );
        opts.server = Some("private-invalid-server".into());
        assert_eq!(error(&opts), "invalid --server URL");
        opts.server = Some("http://127.0.0.1:9876".into());
        assert_eq!(
            error(&opts),
            "cannot read the principal secret file given by --principal-secret-file"
        );
        fs::write(&secret_file, "\r\n").unwrap();
        assert_eq!(error(&opts), "principal secret file is empty");
        fs::write(&secret_file, "private-principal-secret\n").unwrap();
        assert_eq!(
            error(&opts),
            "principal name missing: pass --principal or set NUNYA_PRINCIPAL"
        );
        assert_eq!(
            HelperError::PrincipalSecretMissing.to_string(),
            "principal secret missing: pass --principal-secret-file PATH or set NUNYA_PRINCIPAL_SECRET"
        );
        assert!(
            HelperError::ExpiredWithoutSecret
                .to_string()
                .contains("principal secret is missing")
        );
    }
    #[test]
    fn config_discovery_is_read_only_and_reports_the_selected_path() {
        let temp = Temp::new();
        fs::create_dir(&temp.0).unwrap();
        let local = temp.0.join("nunya.toml");
        let missing = temp.0.join("missing.toml");
        let source = "[upstream]\nhost='localhost'\nuser='nunya'\ndbname='app'\n";
        fs::write(&local, source).unwrap();

        let defaults = ConnectionDefaults::discover(None, &local).unwrap().unwrap();
        assert_eq!(defaults.server, "http://127.0.0.1:6481");
        let secret = temp.0.join("state").join("operator.secret");
        assert_eq!(defaults.operator_secret_file, secret);
        let opts = ConnectionOpts::default();
        let error = opts.connect_with(None, &local, || None).err().unwrap();
        assert_eq!(
            error.to_string(),
            format!("principal secret missing: pass --principal-secret-file, set NUNYA_PRINCIPAL_SECRET, or create {} by running nunya serve once", secret.display())
        );
        assert_eq!(fs::read_dir(&temp.0).unwrap().count(), 1);
        assert_eq!(fs::read_to_string(&local).unwrap(), source);
        let error = ConnectionDefaults::discover(Some(&missing), &local).err().unwrap();
        assert!(error.to_string().contains(&missing.display().to_string()));
        fs::write(&local, "[broken").unwrap();
        let error = ConnectionDefaults::discover(None, &local).err().unwrap();
        assert!(error.to_string().contains(&local.display().to_string()));
    }
    #[test]
    fn management_defaults_use_matching_loopback_family_and_tls_scheme() {
        assert_eq!(
            management_url("0.0.0.0:6481".parse().unwrap(), false),
            "http://127.0.0.1:6481"
        );
        assert_eq!(
            management_url("[::]:6481".parse().unwrap(), true),
            "https://[::1]:6481"
        );
        assert_eq!(
            management_url("192.0.2.24:6481".parse().unwrap(), true),
            "https://192.0.2.24:6481"
        );
    }
    #[test]
    fn rotation_requires_locally_stored_parameters() -> Result<(), Box<dyn std::error::Error>> {
        let old = serde_json::to_value(context())?;
        let mut old = old.as_object().ok_or("missing context object")?.clone();
        old.remove("scram_salt");
        old.remove("scram_iterations");
        let mut ctx: Context = serde_json::from_value(serde_json::Value::Object(old))?;
        for (salt, iterations) in [
            (None, None),
            (Some("c2FsdA==".into()), None),
            (None, Some(4096)),
        ] {
            ctx.scram_salt = salt;
            ctx.scram_iterations = iterations;
            let err = rotation_parameters(&ctx).unwrap_err();
            assert_eq!(err, HelperError::RotationUnavailable);
            assert_eq!(
                err.to_string(),
                "this session was not acquired by this helper; acquire a new session instead"
            );
        }
        Ok(())
    }
    #[test]
    fn management_refusal_display() {
        let error = HelperError::from(ClientError::Api {
            status: 403,
            code: "no_grant".into(),
        });
        assert_eq!(
            error.to_string(),
            "session management request failed: management API 403: no_grant"
        );
    }
    #[test]
    fn shell_quoting() {
        assert_eq!(quoted("a'b", EnvFormat::Sh).as_str(), "'a'\\''b'");
        assert_eq!(quoted("a'b", EnvFormat::Powershell).as_str(), "'a''b'");
    }
    #[test]
    fn concurrent_put_preserves_all_contexts() {
        let temp = Temp::new();
        std::thread::scope(|scope| {
            for worker in 0..2 {
                let dir = &temp.0;
                scope.spawn(move || {
                    let store = HelperStore::at(dir);
                    for index in 0..20 {
                        let mut ctx = context();
                        ctx.name = format!("worker-{worker}-{index}");
                        store.put(&ctx).unwrap();
                    }
                });
            }
        });
        assert_eq!(HelperStore::at(&temp.0).load().unwrap().contexts.len(), 40);
    }
    #[test]
    fn pre_acquire_conflict_is_local() {
        let temp = Temp::new();
        let store = HelperStore::at(&temp.0);
        store.put(&context()).unwrap();
        let _lock = store.lock().unwrap();
        assert_eq!(store.check_available("default"), Err(HelperError::Conflict));
    }
    #[test]
    fn store_round_trip() -> Result<(), HelperError> {
        let temp = Temp::new();
        let store = HelperStore::at(&temp.0);
        let ctx = context();
        store.put(&ctx)?;
        let read = store.get("default")?;
        assert!(
            read.secret.as_str() == ctx.secret.as_str(),
            "stored credential mismatch"
        );
        assert_eq!(read.name, "default");
        assert_eq!(read.scope, ctx.scope);
        assert!(!format!("{read:?}").contains(ctx.secret.as_str()));
        store.put(&read)?;
        store.remove("default")?;
        assert_eq!(store.get("default").unwrap_err(), HelperError::Missing);
        Ok(())
    }
    #[test]
    fn unknown_version() -> Result<(), Box<dyn std::error::Error>> {
        let temp = Temp::new();
        let store = HelperStore::at(&temp.0);
        store.put(&context())?;
        fs::write(
            temp.0.join("sessions.json"),
            br#"{"version":2,"contexts":{}}"#,
        )?;
        assert_eq!(store.get("default").unwrap_err(), HelperError::Version);
        Ok(())
    }
    #[cfg(unix)]
    #[test]
    fn unix_permissions() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;
        let temp = Temp::new();
        let store = HelperStore::at(&temp.0);
        store.put(&context())?;
        let file = temp.0.join("sessions.json");
        assert_eq!(fs::metadata(&file)?.permissions().mode() & 0o777, 0o600);
        assert_eq!(fs::metadata(&temp.0)?.permissions().mode() & 0o777, 0o700);
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644))?;
        assert_eq!(store.get("default").unwrap_err(), HelperError::Store);
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600))?;
        fs::set_permissions(&temp.0, fs::Permissions::from_mode(0o755))?;
        assert_eq!(store.get("default").unwrap_err(), HelperError::Store);
        Ok(())
    }
}
