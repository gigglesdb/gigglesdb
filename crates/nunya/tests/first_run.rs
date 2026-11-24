//! First-start behavior with an in-process Spanner catalog endpoint.
use crate::common::{NativeUpstream, ServeHarness, ServeResult};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
    BatchWriteResponse, CreateSessionRequest, DeleteSessionRequest, ExecuteSqlRequest,
    PartialResultSet, ResultSet, Session,
};
use nunya::{
    mgmt::client::{Credentials, MgmtClient},
    policy::{RouteDefault, file::parse_policy_document},
    spanner::server::{SpannerServer, SpannerService},
    state::{model::PrincipalKind, schema::STORE_FILE},
    token::SessionId,
};
use std::{
    fs,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    time::Duration,
};
use tonic::{Request, Response, Status};
use zeroize::Zeroizing;

const DATABASE: &str = "projects/p/instances/i/databases/d";
const MINIMAL_CONFIG: &str =
    "[spanner_native]\nroute_id = \"data\"\ndatabase = \"projects/p/instances/i/databases/d\"\n";

struct Catalog;

#[tonic::async_trait]
impl SpannerService for Catalog {
    type ExecuteStreamingSqlStream = futures::stream::Empty<Result<PartialResultSet, Status>>;
    type StreamingReadStream = Self::ExecuteStreamingSqlStream;
    type BatchWriteStream = futures::stream::Empty<Result<BatchWriteResponse, Status>>;

    async fn create_session(
        &self,
        request: Request<CreateSessionRequest>,
    ) -> Result<Response<Session>, Status> {
        Ok(Response::new(Session {
            name: format!("{}/sessions/catalog", request.into_inner().database),
            ..Default::default()
        }))
    }

    async fn execute_sql(
        &self,
        _request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ResultSet>, Status> {
        Ok(Response::new(ResultSet::default()))
    }

    async fn delete_session(
        &self,
        _request: Request<DeleteSessionRequest>,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }
}

struct CatalogServer(tokio::task::JoinHandle<()>);

impl CatalogServer {
    async fn start() -> ServeResult<(Self, String)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let incoming = futures::stream::unfold(listener, |listener| async move {
            let result = listener.accept().await.map(|(socket, _)| socket);
            Some((result, listener))
        });
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(SpannerServer::new(Catalog))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        Ok((Self(task), endpoint))
    }
}

impl Drop for CatalogServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> ServeResult<Self> {
        let path = std::env::temp_dir().join(format!("nf7-{}", SessionId::random().to_hex()));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Server(Option<Child>);

impl Server {
    fn start(config: &Path) -> ServeResult<Self> {
        Self::spawn(nunya(config, &["serve"]))
    }

    fn spawn(mut command: Command) -> ServeResult<Self> {
        let child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        Ok(Self(Some(child)))
    }

    fn child(&mut self) -> &mut Child {
        self.0.as_mut().unwrap()
    }

    fn finish(mut self) -> ServeResult<Output> {
        if self.child().try_wait()?.is_none() {
            self.child().kill()?;
        }
        Ok(self.0.take().unwrap().wait_with_output()?)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn nunya(config: &Path, command: &[&str]) -> Command {
    let mut process = Command::new(env!("CARGO_BIN_EXE_nunya"));
    process
        .args(command)
        .arg("--config")
        .arg(config)
        .env_remove("NUNYA_ROOT_KEY");
    process
}

fn operator_client(address: SocketAddr, secret: &str) -> ServeResult<MgmtClient> {
    Ok(MgmtClient::new(
        format!("http://{address}").parse()?,
        None,
        Credentials::Bearer {
            name: "operator".into(),
            secret: Zeroizing::new(secret.into()),
        },
    )?)
}

fn local_cli(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_nunya"));
    command
        .current_dir(dir)
        .env("NUNYA_HOME", dir.join("helper"))
        .env_remove("NUNYA_CONFIG")
        .env_remove("NUNYA_SERVER")
        .env_remove("NUNYA_PRINCIPAL")
        .env_remove("NUNYA_PRINCIPAL_SECRET")
        .env_remove("NUNYA_CA_FILE");
    command
}

fn cli_success(command: &mut Command) -> ServeResult<Output> {
    let output = command.output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output)
}

async fn ready(server: &mut Server, native: SocketAddr) -> ServeResult<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = server.child().try_wait()? {
                let output = server.0.take().unwrap().wait_with_output()?;
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(format!("serve exited during startup: {status}: {stderr}").into());
            }
            if tokio::net::TcpStream::connect(native).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

fn listing(path: &Path) -> ServeResult<Vec<PathBuf>> {
    let mut entries = fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort();
    Ok(entries)
}

#[tokio::test]
async fn first_start_persists_credentials_policy_and_principals() -> ServeResult<()> {
    let (_catalog, endpoint) = CatalogServer::start().await?;
    let dir = TestDir::new()?;
    let config = dir.0.join("nunya.toml");
    let ports = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0"))
        .collect::<Result<Vec<_>, _>>()?;
    let (management, admin, native) = (
        ports[0].local_addr()?,
        ports[1].local_addr()?,
        ports[2].local_addr()?,
    );
    fs::write(
        &config,
        format!(
            "admin_listen = '{admin}'\n[management]\nlisten = '{management}'\n{MINIMAL_CONFIG}listen = '{native}'\nemulator_endpoint = '{endpoint}'\n"
        ),
    )?;
    drop(ports);
    let state = dir.0.join("state");
    let key_path = state.join("root.key");
    let secret_path = state.join("operator.secret");
    let policy_path = dir.0.join("policy.toml");
    assert!(!state.exists());

    let mut server = Server::start(&config)?;
    ready(&mut server, native).await?;
    let key = fs::read_to_string(&key_path)?;
    let secret = fs::read_to_string(&secret_path)?;
    for line in [&key, &secret] {
        assert_eq!(line.len(), 65);
        assert!(line.ends_with('\n'));
        assert!(line[..64].bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
    let policy = fs::read_to_string(&policy_path)?;
    let document = parse_policy_document(&policy).unwrap();
    assert_eq!(document.route_default, RouteDefault::Tokenize);
    assert!(document.policy.columns.is_empty());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(fs::metadata(&state)?.permissions().mode() & 0o777, 0o700);
        for path in [&key_path, &secret_path] {
            assert_eq!(fs::metadata(path)?.permissions().mode() & 0o777, 0o600);
        }
    }

    let operator = operator_client(management, secret.trim_end())?;
    let principals = operator.principals().await?;
    assert_eq!(principals.len(), 1);
    assert_eq!(principals[0].name, "operator");
    assert!(principals[0].administrator);
    assert_eq!(
        operator
            .principal(&principals[0].id)
            .await?
            .credentials
            .len(),
        1
    );
    let persisted = operator
        .create_principal("existing", PrincipalKind::Person, false, None)
        .await?;
    cli_success(local_cli(&dir.0).args(["connections", "list"]))?;
    cli_success(local_cli(&dir.0).args([
        "principal", "grant", "operator", "--route", "data=read-only",
    ]))?;
    cli_success(local_cli(&dir.0).args([
        "session", "acquire", "--context", "bootstrap", "--route", "data",
    ]))?;
    let output = local_cli(&dir.0)
        .args(["session", "acquire", "--context", "bootstrap", "--route", "data"])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("principal secret missing"));
    cli_success(
        local_cli(&dir.0)
            .args(["session", "acquire", "--context", "bootstrap", "--principal-secret-file"])
            .arg(&secret_path),
    )?;
    cli_success(local_cli(&dir.0).args([
        "principal", "create", "reporting-agent", "--kind", "agent", "--generate-secret",
        "--secret-out", "principal.secret",
    ]))?;
    cli_success(local_cli(&dir.0).args([
        "principal", "grant", "reporting-agent", "--route", "data=read-only",
    ]))?;
    cli_success(local_cli(&dir.0).args([
        "session", "acquire", "--principal", "reporting-agent", "--principal-secret-file",
        "principal.secret", "--route", "data", "--ttl", "1h", "--label", "reporting",
    ]))?;
    for verb in ["env", "show"] {
        cli_success(local_cli(&dir.0).args(["session", verb]))?;
    }
    for prefix in [&["exec"][..], &["session", "exec"][..]] {
        let run = |opts: &[&str]| {
            local_cli(&dir.0)
                .args(prefix)
                .args(opts)
                .args(["--", env!("CARGO_BIN_EXE_nunya"), "--help"])
                .output()
        };
        for opts in [
            &[][..],
            &["--route", "data", "--mode", "read-only", "--ttl", "60m", "--label", "reporting"],
        ] {
            let output = run(opts)?;
            let error = String::from_utf8(output.stderr)?;
            assert!(output.status.success(), "{error}");
            assert!(error.contains("session not revalidated"));
            assert!(String::from_utf8(output.stdout)?.contains("Usage:"));
        }
        for (flag, value) in [
            ("--route", "postgres"),
            ("--mode", "read-write"),
            ("--ttl", "2h"),
            ("--label", "other"),
        ] {
            let output = run(&[flag, value])?;
            assert!(!output.status.success());
            assert!(output.stdout.is_empty());
            assert!(String::from_utf8(output.stderr)?.contains(&format!(
                "cannot reuse stored session with {flag}: the stored value differs or is unknown, and the principal secret is missing"
            )));
        }
    }
    let output = server.finish()?;
    let logs = [output.stdout, output.stderr].concat();
    let logs = String::from_utf8_lossy(&logs);
    for path in [&key_path, &secret_path, &policy_path] {
        assert!(logs.contains(path.to_str().unwrap()));
    }
    assert!(!logs.contains(key.trim_end()));
    assert!(!logs.contains(secret.trim_end()));

    let mut restarted = Server::start(&config)?;
    ready(&mut restarted, native).await?;
    assert_eq!(fs::read_to_string(&key_path)?, key);
    assert_eq!(fs::read_to_string(&secret_path)?, secret);
    assert_eq!(fs::read_to_string(&policy_path)?, policy);
    assert_eq!(operator.principal(&persisted.id).await?.name, "existing");
    restarted.finish()?;

    fs::remove_file(&secret_path)?;
    let mut restarted = Server::start(&config)?;
    ready(&mut restarted, native).await?;
    assert!(!secret_path.exists());
    assert_eq!(operator.principal(&persisted.id).await?.name, "existing");
    restarted.finish()?;

    fs::remove_file(&key_path)?;
    assert!(state.join(STORE_FILE).is_file());
    let before = (listing(&dir.0)?, listing(&state)?);
    let mut refused = Server::start(&config)?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while refused.child().try_wait()?.is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<(), std::io::Error>(())
    })
    .await??;
    let output = refused.finish()?;
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr)?;
    assert!(error.contains(&format!(
        "root key file {} is missing but state in {} already exists; restore the key or set NUNYA_ROOT_KEY / root_key_file",
        key_path.display(), state.display(),
    )));
    assert_eq!((listing(&dir.0)?, listing(&state)?), before);
    Ok(())
}

#[tokio::test]
async fn interrupted_bootstrap_recovers_secret_and_cli_honors_connection_precedence() -> ServeResult<()> {
    let (_catalog, endpoint) = CatalogServer::start().await?;
    let dir = TestDir::new()?;
    let config = dir.0.join("nunya.toml");
    let invalid_config = dir.0.join("invalid.toml");
    fs::write(&invalid_config, "[invalid")?;
    let ports = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0"))
        .collect::<Result<Vec<_>, _>>()?;
    let (management, admin, native) = (
        ports[0].local_addr()?,
        ports[1].local_addr()?,
        ports[2].local_addr()?,
    );
    fs::write(
        &config,
        format!(
            "admin_listen = '{admin}'\n[management]\nlisten = '{management}'\n{MINIMAL_CONFIG}listen = '{native}'\n"
        ),
    )?;
    drop(ports);
    let state = dir.0.join("state");
    fs::create_dir(&state)?;
    let secret_path = state.join("operator.secret");
    let secret = "recovered-operator-password";
    let original = format!("{secret}\r\n");
    fs::write(&secret_path, &original)?;
    let mut command = nunya(&config, &["serve"]);
    command.env("SPANNER_EMULATOR_HOST", endpoint.strip_prefix("http://").unwrap());
    let mut server = Server::spawn(command)?;
    ready(&mut server, native).await?;
    let operator = operator_client(management, secret)?;
    assert!(operator.principals().await?[0].administrator);
    assert_eq!(fs::read_to_string(&secret_path)?, original);

    // The environment config wins even when the working directory contains invalid TOML.
    let elsewhere = dir.0.join("elsewhere");
    fs::create_dir(&elsewhere)?;
    fs::copy(&invalid_config, elsewhere.join("nunya.toml"))?;
    cli_success(
        local_cli(&elsewhere)
            .env("NUNYA_CONFIG", &config)
            .args(["principal", "list"]),
    )?;
    cli_success(
        local_cli(&elsewhere)
            .env("NUNYA_CONFIG", &invalid_config)
            .args(["principal", "list", "--config"])
            .arg(&config),
    )?;
    let server_url = format!("http://{management}");
    let different_server = "http://127.0.0.1:1";
    for explicit in [true, false] {
        let mut command = local_cli(&dir.0);
        command.args(["principal", "list"]);
        if explicit {
            command.args(["--server", different_server]);
        } else {
            command.env("NUNYA_SERVER", different_server);
        }
        let output = command.output()?;
        assert!(!output.status.success());
        assert_eq!(
            String::from_utf8(output.stderr)?.trim(),
            "principal name missing: pass --principal or set NUNYA_PRINCIPAL",
        );
    }
    let env_command = || {
        let mut command = local_cli(&dir.0);
        command
            .args(["principal", "list"])
            .env("NUNYA_SERVER", &server_url)
            .env("NUNYA_PRINCIPAL", "operator")
            .env("NUNYA_PRINCIPAL_SECRET", secret);
        command
    };
    let env_override = env_command().env("NUNYA_SERVER", "bad-server").output()?;
    assert!(!env_override.status.success());
    assert!(String::from_utf8(env_override.stderr)?.contains("invalid --server URL"));
    let env_override = env_command().env("NUNYA_PRINCIPAL", "bad-principal").output()?;
    assert!(!env_override.status.success());
    let env_override = env_command().env("NUNYA_PRINCIPAL_SECRET", "bad-secret").output()?;
    assert!(!env_override.status.success());
    cli_success(env_command().env("NUNYA_CONFIG", &invalid_config))?;
    cli_success(
        env_command()
            .env("NUNYA_CONFIG", &invalid_config)
            .env("NUNYA_SERVER", "bad-server")
            .env("NUNYA_PRINCIPAL", "bad-principal")
            .env("NUNYA_PRINCIPAL_SECRET", "bad-secret")
            .args(["--server", &server_url, "--principal", "operator", "--principal-secret-file"])
            .arg(&secret_path),
    )?;

    let output = server.finish()?;
    let logs = String::from_utf8(output.stderr)?;
    assert!(logs.contains(&format!(
        "created administrator operator from existing secret in {}", secret_path.display()
    )));
    assert!(logs.contains(&format!("using Spanner emulator at {endpoint} from SPANNER_EMULATOR_HOST")));
    assert!(!logs.contains(secret));
    Ok(())
}

#[tokio::test]
async fn configured_admin_and_explicit_key_skip_local_credentials() -> ServeResult<()> {
    let (_catalog, endpoint) = CatalogServer::start().await?;
    let mut harness = ServeHarness::new(
        "[readlog]\nsink = 'stdout'\n",
        None,
        None,
        Some(NativeUpstream {
            database: DATABASE,
            emulator_endpoint: &endpoint,
        }),
        "",
    )?;
    harness.start().await?;
    assert!(!harness.state_dir().join("root.key").exists());
    assert!(!harness.state_dir().join("operator.secret").exists());
    assert!(harness.admin().principals().await?.is_empty());
    Ok(())
}

#[test]
fn policy_check_validates_default_without_writing() -> ServeResult<()> {
    let dir = TestDir::new()?;
    let config = dir.0.join("nunya.toml");
    fs::write(&config, MINIMAL_CONFIG)?;
    let before = listing(&dir.0)?;
    let output = nunya(&config, &["policy", "check"])
        .env("SPANNER_EMULATOR_HOST", "invalid")
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "policy OK");
    assert_eq!(listing(&dir.0)?, before);

    fs::write(
        &config,
        format!("[readlog]\nsink='file'\npath='audit.log'\n{MINIMAL_CONFIG}"),
    )?;
    let output = nunya(&config, &["policy", "check"]).output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(listing(&dir.0)?, before);

    fs::write(&config, format!("policy='policy.toml'\n{MINIMAL_CONFIG}"))?;
    let output = nunya(&config, &["policy", "check"]).output()?;
    assert!(!output.status.success());
    let policy = dir.0.join("policy.toml");
    assert!(String::from_utf8_lossy(&output.stderr).contains(&format!(
        "policy file unavailable: {}", policy.display()
    )));
    assert_eq!(listing(&dir.0)?, before);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policy_check_uses_spanner_emulator_without_writing() -> ServeResult<()> {
    let (_catalog, endpoint) = CatalogServer::start().await?;
    let dir = TestDir::new()?;
    let config = dir.0.join("nunya.toml");
    let source = format!("policy='policy.toml'\n{MINIMAL_CONFIG}");
    fs::write(&config, &source)?;
    fs::write(
        dir.0.join("policy.toml"),
        "[readlog]\nsink='file'\npath='audit.log'\n",
    )?;
    let before = listing(&dir.0)?;
    let check = |host| {
        nunya(&config, &["policy", "check"])
            .env("SPANNER_EMULATOR_HOST", host)
            .output()
    };
    let output = check(endpoint.strip_prefix("http://").unwrap())?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let output = check("invalid")?;
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("SPANNER_EMULATOR_HOST must be host:port"));

    fs::write(&config, format!("{source}emulator_endpoint='{endpoint}'\n"))?;
    let output = check("invalid")?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(listing(&dir.0)?, before);
    Ok(())
}
