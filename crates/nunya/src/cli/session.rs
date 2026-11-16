//! Owner-private session persistence and PostgreSQL command helpers.
use crate::{
    config::parse_duration,
    mgmt::client::{
        AcquireParams, ClientError, Credentials, MgmtClient, SessionSecret, SessionView,
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
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

const ENDED: &str = "the stored session has ended and its token scope was replaced; tokens from it will not match a new session. Run again with --new-scope to start a new scope.";
/// Fixed, secret-free helper failures.
#[derive(Debug, PartialEq, Eq)]
pub enum HelperError {
    /// Stored scope cannot be resumed.
    ScopeReplaced,
    /// Storage unavailable or insecure.
    Store,
    /// Unsupported helper format.
    Version,
    /// Context does not exist.
    Missing,
    /// Context would be overwritten or used with another identity.
    Conflict,
    /// Connection configuration invalid.
    Configuration,
    /// Management request refused or unavailable.
    Management,
    /// Child could not be started.
    Child,
}
impl fmt::Display for HelperError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ScopeReplaced => ENDED,
            Self::Store => "session helper store unavailable or insecure",
            Self::Version => "unsupported session helper version",
            Self::Missing => "session context not found",
            Self::Conflict => "session context already exists or belongs to another connection",
            Self::Configuration => "invalid session helper configuration",
            Self::Management => "session management request failed",
            Self::Child => "unable to start child command",
        })
    }
}
impl std::error::Error for HelperError {}
impl From<ClientError> for HelperError {
    fn from(_: ClientError) -> Self {
        Self::Management
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
    /// Session identifier.
    pub session_id: String,
    /// Credential protected on drop.
    #[serde(with = "secret_serde")]
    pub secret: Zeroizing<String>,
    /// Scope identifier.
    pub scope: String,
    /// Expiration timestamp.
    pub expires_ms: u64,
    /// PostgreSQL host.
    pub pg_host: String,
    /// PostgreSQL port.
    pub pg_port: u16,
    /// Scope isolation flag.
    pub isolated: bool,
    /// Optional label.
    pub label: Option<String>,
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
        let mut data = self.load()?;
        data.contexts.insert(ctx.name.clone(), ctx.clone());
        self.save(&data)
    }
    /// Delete a context.
    pub fn remove(&self, name: &str) -> Result<(), HelperError> {
        let mut data = self.load()?;
        data.contexts.remove(name);
        self.save(&data)
    }
}
/// Common management connection options.
#[derive(Args, Clone)]
pub struct ConnectionOpts {
    /// Management URL.
    #[arg(long, env = "NUNYA_SERVER")]
    pub server: String,
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
impl ConnectionOpts {
    fn name(&self) -> &str {
        self.context.as_deref().unwrap_or("default")
    }
    fn client(&self) -> Result<MgmtClient, HelperError> {
        let credentials = if self.server.starts_with("unix:") {
            Credentials::None
        } else {
            let secret = if let Some(path) = &self.principal_secret_file {
                let file = fs::File::open(path).map_err(|_| HelperError::Configuration)?;
                let mut text = Zeroizing::new(String::new());
                std::io::BufReader::new(file)
                    .read_line(&mut text)
                    .map_err(|_| HelperError::Configuration)?;
                while text.ends_with(['\r', '\n']) {
                    text.pop();
                }
                text
            } else {
                Zeroizing::new(
                    std::env::var("NUNYA_PRINCIPAL_SECRET")
                        .map_err(|_| HelperError::Configuration)?,
                )
            };
            if secret.is_empty() {
                return Err(HelperError::Configuration);
            }
            Credentials::Bearer {
                name: self.principal.clone().ok_or(HelperError::Configuration)?,
                secret,
            }
        };
        MgmtClient::new(
            self.server
                .parse()
                .map_err(|_| HelperError::Configuration)?,
            self.ca_file.as_deref(),
            credentials,
        )
        .map_err(Into::into)
    }
    fn context(&self, store: &HelperStore) -> Result<Context, HelperError> {
        let ctx = store.get(self.name())?;
        if ctx.server != self.server || ctx.principal != self.principal {
            return Err(HelperError::Conflict);
        }
        Ok(ctx)
    }
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
    /// Route identifier.
    #[arg(long, default_value = "postgres")]
    pub route: String,
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
    /// PostgreSQL host.
    #[arg(long, env = "NUNYA_PG_HOST", default_value = "127.0.0.1")]
    pub pg_host: String,
    /// PostgreSQL port.
    #[arg(long, env = "NUNYA_PG_PORT", default_value_t = 6432)]
    pub pg_port: u16,
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
        #[arg(long)]
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
            HelperError::Management
        }
    })?;
    if view.state != SessionState::Active || view.scope != ctx.scope {
        return Err(HelperError::ScopeReplaced);
    }
    // Renewal authoritatively checks expiration using the server's clock.
    let view = client
        .renew(&ctx.session_id, None, None)
        .await
        .map_err(|e| {
            if ended(&e) {
                HelperError::ScopeReplaced
            } else {
                HelperError::Management
            }
        })?;
    ctx.expires_ms = view.expires_ms;
    Ok(view)
}
/// Acquire a new session or authoritatively revalidate a stored session.
pub async fn acquire_or_resume(
    client: &MgmtClient,
    store: &HelperStore,
    opts: &AcquireOpts,
) -> Result<Context, HelperError> {
    let name = opts.connection.name();
    let existing = store.load()?.contexts.contains_key(name);
    if opts.isolated {
        if opts.connection.context.is_some() && existing {
            return Err(HelperError::Conflict);
        }
    } else if existing {
        let mut ctx = opts.connection.context(store)?;
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
            route: opts.route.clone(),
            mode: opts.mode,
            lifetime: opts.ttl.clone(),
            isolated: opts.isolated,
            label: opts.label.clone(),
            verifier: secret.verifier,
            secret_sha256: secret.sha256_hex,
        })
        .await?;
    let name = if opts.isolated && opts.connection.context.is_none() {
        format!(
            "isolated-{}",
            view.id.get(..8).ok_or(HelperError::Management)?
        )
    } else {
        name.into()
    };
    if store.load()?.contexts.contains_key(&name) {
        return Err(HelperError::Conflict);
    }
    let ctx = Context {
        name,
        server: opts.connection.server.clone(),
        principal: opts.connection.principal.clone(),
        route: view.route,
        session_id: view.id,
        secret: secret.secret,
        scope: view.scope,
        expires_ms: view.expires_ms,
        pg_host: opts.pg_host.clone(),
        pg_port: opts.pg_port,
        isolated: view.isolated,
        label: view.label,
    };
    store.put(&ctx)?;
    Ok(ctx)
}
/// PostgreSQL environment values, including the protected password.
pub fn pg_env(ctx: &Context) -> Vec<(String, Zeroizing<String>)> {
    [
        ("PGHOST", ctx.pg_host.clone()),
        ("PGPORT", ctx.pg_port.to_string()),
        ("PGUSER", format!("session:{}", ctx.session_id)),
        ("PGPASSWORD", ctx.secret.to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), Zeroizing::new(v)))
    .collect()
}
fn quoted(value: &str, format: EnvFormat) -> Zeroizing<String> {
    Zeroizing::new(match format {
        EnvFormat::Sh => format!("'{}'", value.replace('\'', "'\\''")),
        EnvFormat::Powershell => format!("'{}'", value.replace('\'', "''")),
    })
}
async fn summary(client: &MgmtClient, ctx: &Context) -> Result<(), HelperError> {
    let view = client.session(&ctx.session_id).await?;
    println!(
        "id: {}\nroute: {}\nmode: {:?}\nexpires_ms: {}\ncontext: {}",
        view.id, view.route, view.mode, view.expires_ms, ctx.name
    );
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
            let client = opts.connection.client()?;
            let ctx = acquire_or_resume(&client, &store, &opts).await?;
            summary(&client, &ctx).await?;
        }
        SessionCommand::Show(connection) => {
            let client = connection.client()?;
            let ctx = connection.context(&store)?;
            let v = client.session(&ctx.session_id).await?;
            println!(
                "state: {:?}\nmode: {:?}\nexpires_ms: {}\nrevoke_reason: {:?}\nscope_continuity: {}",
                v.state, v.mode, v.expires_ms, v.revoke_reason, v.scope_continuity
            );
        }
        SessionCommand::Renew { connection, mode } => {
            let client = connection.client()?;
            let mut ctx = connection.context(&store)?;
            let view = client.renew(&ctx.session_id, mode, None).await?;
            ctx.expires_ms = view.expires_ms;
            store.put(&ctx)?;
            summary(&client, &ctx).await?;
        }
        SessionCommand::Revoke(connection) => {
            let client = connection.client()?;
            let ctx = connection.context(&store)?;
            revoke(&client, &store, &ctx).await?;
            println!("session revoked");
        }
        SessionCommand::Env { connection, format } => {
            let client = connection.client()?;
            let mut ctx = connection.context(&store)?;
            resume(&client, &mut ctx).await?;
            store.put(&ctx)?;
            for (key, value) in pg_env(&ctx) {
                let value = quoted(&value, format);
                match format {
                    EnvFormat::Sh => println!("export {key}={}", value.as_str()),
                    EnvFormat::Powershell => println!("$env:{key} = {}", value.as_str()),
                }
            }
        }
    }
    Ok(0)
}
fn runtime(result: impl std::future::Future<Output = Result<i32, HelperError>>) -> i32 {
    let result = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt.block_on(result),
        Err(_) => Err(HelperError::Configuration),
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
/// Acquire a session and execute a child with inherited standard streams.
pub fn run_exec(opts: ExecOpts) -> i32 {
    runtime(async {
        let client = opts.opts.connection.client()?;
        let store = HelperStore::discover()?;
        let ctx = acquire_or_resume(&client, &store, &opts.opts).await?;
        let program = opts.command.first().ok_or(HelperError::Child)?;
        let mut child = std::process::Command::new(program);
        child.args(&opts.command[1..]);
        for (key, value) in pg_env(&ctx) {
            child.env(key, value.as_str());
        }
        let status = child.status().map_err(|_| HelperError::Child)?;
        Ok(status.code().unwrap_or(1))
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            Self(
                PathBuf::from("J:/gigglesdb-work/nunya/target-w2-tmp").join(format!(
                    "helper-{}",
                    crate::token::SessionId::random().to_hex()
                )),
            )
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
            session_id: "12".repeat(16),
            secret: Zeroizing::new("private-session-secret".into()),
            scope: "34".repeat(16),
            expires_ms: 12345,
            pg_host: "localhost".into(),
            pg_port: 6432,
            isolated: false,
            label: None,
        }
    }
    #[test]
    fn shell_quoting() {
        assert_eq!(quoted("a'b", EnvFormat::Sh).as_str(), "'a'\\''b'");
        assert_eq!(quoted("a'b", EnvFormat::Powershell).as_str(), "'a''b'");
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
