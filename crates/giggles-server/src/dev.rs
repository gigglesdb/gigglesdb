//! Explicit, loopback-only development server provisioning.
use crate::{
    Service,
    api::{ApiLimits, ApiRuntime, net::serve_role_bound},
    auth::{PostgresqlConfig, RoleApiConfig, TlsMode, scram::Verifier, serve_postgresql_bound},
    clock::Clock,
};
use giggles_core::{
    Admin, Engine, Limits,
    roles::{NewRole, RoleChanges},
};
use std::{
    io::{self, Write},
    path::PathBuf,
};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::watch};

#[cfg(feature = "engine-rocksdb")]
const DEFAULT_ENGINE: Engine = Engine::V2;
#[cfg(not(feature = "engine-rocksdb"))]
const DEFAULT_ENGINE: Engine = Engine::Legacy;
const API_CONNECTIONS: usize = 32;

pub const USAGE: &str = "usage: giggles-server [--no-stdin-control] CONFIG.json | --dev [--data DIR] [--port N] [--max-connections N] [--engine legacy|v2] [--owner NAME] [--database NAME] [--trust-local] [--no-api] [--reset-owner-password] (max-connections: 1-128, default 100; creation default: v2 with engine-rocksdb, otherwise legacy)";

#[derive(Debug, PartialEq, Eq)]
pub struct Options {
    pub data: PathBuf,
    pub port: u16,
    pub max_connections: usize,
    /// Creation only; existing directories are detected by their format.
    pub engine: Engine,
    pub owner: String,
    pub database: String,
    pub trust_local: bool,
    pub no_api: bool,
    pub reset_owner_password: bool,
}

impl Options {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, &'static str> {
        let mut options = Self {
            data: "giggles-data".into(),
            port: 5432,
            max_connections: 100,
            engine: DEFAULT_ENGINE,
            owner: "postgres".into(),
            database: "postgres".into(),
            trust_local: false,
            no_api: false,
            reset_owner_password: false,
        };
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--trust-local" => options.trust_local = true,
                "--no-api" => options.no_api = true,
                "--reset-owner-password" => options.reset_owner_password = true,
                "--data" | "--port" | "--max-connections" | "--owner" | "--database" | "--engine" => {
                    let value = args
                        .next()
                        .filter(|value| !value.is_empty() && !value.starts_with("--"))
                        .ok_or(USAGE)?;
                    match flag.as_str() {
                        "--data" => options.data = value.into(),
                        "--engine" => {
                            options.engine = match value.as_str() {
                                "legacy" => Engine::Legacy,
                                "v2" => Engine::V2,
                                _ => return Err(USAGE),
                            };
                        }
                        "--port" => {
                            if !value.bytes().all(|byte| byte.is_ascii_digit()) {
                                return Err(USAGE);
                            }
                            options.port = value.parse().map_err(|_| USAGE)?;
                        }
                        "--max-connections" => {
                            if !value.bytes().all(|byte| byte.is_ascii_digit()) {
                                return Err(USAGE);
                            }
                            let connections = value.parse().map_err(|_| USAGE)?;
                            if !(1..=128).contains(&connections) {
                                return Err(USAGE);
                            }
                            options.max_connections = connections;
                        }
                        "--owner" => options.owner = value,
                        "--database" => options.database = value,
                        _ => unreachable!(),
                    }
                }
                _ => return Err(USAGE),
            }
        }
        if !valid_name(&options.owner)
            || !valid_name(&options.database)
            || (!options.no_api && options.port > u16::MAX - 2)
        {
            return Err(USAGE);
        }
        Ok(options)
    }
}

fn valid_name(name: &str) -> bool {
    (1..=63).contains(&name.len())
        && !name.as_bytes()[0].is_ascii_digit()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn random_password() -> io::Result<String> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut password = String::with_capacity(24);
    while password.len() < 24 {
        let mut bytes = [0; 32];
        getrandom::fill(&mut bytes).map_err(io::Error::other)?;
        for byte in bytes {
            // Rejection sampling avoids bias from reducing all 256 values modulo 62.
            if byte < 248 {
                password.push(ALPHABET[usize::from(byte % 62)] as char);
                if password.len() == 24 {
                    break;
                }
            }
        }
    }
    Ok(password)
}

fn percent_encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(encoded, "%{byte:02X}").expect("string write");
        }
    }
    encoded
}

fn bootstrap_owner(
    admin: &Admin,
    owner: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let roles = admin.roles();
    let password = if roles.is_empty()? {
        let password = owner_password()?;
        let verifier = password_verifier(&password)?;
        roles.create(NewRole {
            inherit: None,
            name: owner.to_owned(),
            superuser: true,
            login: true,
            createrole: true,
            password: Some(verifier),
        })?;
        Some(password)
    } else {
        if roles.get(owner)?.is_none() {
            eprintln!("warning: requested owner role is absent");
        }
        None
    };
    Ok(password)
}

fn owner_password() -> Result<String, Box<dyn std::error::Error>> {
    match std::env::var("GIGGLES_OWNER_PASSWORD") {
        Ok(value) if !value.is_empty() => Ok(value),
        Ok(_) | Err(std::env::VarError::NotPresent) => Ok(random_password()?),
        Err(error) => Err(error.into()),
    }
}

pub fn password_verifier(password: &str) -> io::Result<String> {
    let mut salt = [0; 16];
    getrandom::fill(&mut salt).map_err(io::Error::other)?;
    Ok(Verifier::derive(password, &salt, crate::auth::scram::DEFAULT_ITERATIONS).encode())
}

fn reset_owner_password(admin: &Admin, owner: &str) -> Result<String, Box<dyn std::error::Error>> {
    let roles = admin.roles();
    if roles.get(owner)?.is_none() {
        return Err(
            io::Error::new(io::ErrorKind::NotFound, "requested owner role is absent").into(),
        );
    }
    let password = owner_password()?;
    roles.alter(
        owner,
        RoleChanges {
            password: Some(Some(password_verifier(&password)?)),
            ..Default::default()
        },
    )?;
    Ok(password)
}

fn credential_url(
    options: &Options,
    address: std::net::SocketAddr,
    password: Option<&str>,
) -> String {
    let credentials = password
        .map(|password| format!(":{}", percent_encode(password)))
        .unwrap_or_default();
    format!(
        "postgres://{}{credentials}@{address}/{}",
        percent_encode(&options.owner),
        percent_encode(&options.database)
    )
}

fn password_notice(options: &Options) {
    let data = options.data.to_string_lossy();
    let data = if data.chars().any(char::is_whitespace) || data.contains('"') {
        format!("\"{}\"", data.replace('"', "\\\""))
    } else {
        data.into_owned()
    };
    eprintln!(
        "owner password shown once; reset with: giggles-server --dev --data {data} --reset-owner-password"
    );
}

fn open_existing(path: &std::path::Path) -> Result<Admin, Box<dyn std::error::Error>> {
    Admin::open(path, Limits::default()).map_err(|error| {
        if matches!(&error, giggles_core::CoreError::Storage(detail) if detail.contains("Database already open. Cannot acquire lock.")
                || detail.contains("database already open by another writer"))
        {
            io::Error::other(format!("{error}; stop the running server first")).into()
        } else {
            Box::new(error) as Box<dyn std::error::Error>
        }
    })
}

pub async fn run(options: Options) -> Result<(), Box<dyn std::error::Error>> {
    let path = options.data.join("db");
    if options.reset_owner_password {
        let admin = open_existing(&path)?;
        let password = reset_owner_password(&admin, &options.owner)?;
        let mut stdout = io::stdout().lock();
        writeln!(
            stdout,
            "{}",
            credential_url(
                &options,
                ([127, 0, 0, 1], options.port).into(),
                Some(&password)
            )
        )?;
        stdout.flush()?;
        password_notice(&options);
        return Ok(());
    }
    std::fs::create_dir_all(&options.data)?;
    let existing = path.try_exists()?;
    let admin = if existing {
        open_existing(&path)?
    } else {
        Admin::create_engine(&path, Limits::default(), options.engine)?
    };
    let password = bootstrap_owner(&admin, &options.owner)?;
    let service = Service::new(admin, Vec::new(), Clock::new(0, 3600)?)?;
    let config = PostgresqlConfig {
        listen: ([127, 0, 0, 1], options.port).into(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: Default::default(),
        databases: vec![options.database.clone()],
        loopback_trust: options.trust_local,
        connections: options.max_connections,
    };
    let listener = std::sync::Arc::new(config.build()?);
    let socket = TcpListener::bind(config.listen).await?;
    // Bind every socket before starting accept tasks, so partial bind failures are clean.
    let mut api_sockets = Vec::new();
    if !options.no_api {
        for grpc in [false, true] {
            let port = if options.port == 0 {
                0
            } else {
                options.port + if grpc { 2 } else { 1 }
            };
            let address = ([127, 0, 0, 1], port).into();
            let config = RoleApiConfig {
                http: (!grpc).then_some(address),
                grpc: grpc.then_some(address),
                wire: None,
                tls: TlsMode::Disabled,
                wire_tls: None,
                certificate: None,
                private_key: None,
                client_ca: None,
                certificates: Default::default(),
                databases: config.databases.clone(),
                loopback_trust: config.loopback_trust,
                connections: API_CONNECTIONS,
                idle_timeout_seconds: None,
                max_age_seconds: None,
            };
            let settings = std::sync::Arc::new(config.build()?);
            let runtime =
                ApiRuntime::new_role(service.clone(), ApiLimits::default(), settings.clone())?;
            api_sockets.push((grpc, TcpListener::bind(address).await?, settings, runtime));
        }
    }
    let (start, gate) = watch::channel(false);
    let running = serve_postgresql_bound(socket, service.clone(), listener, gate.clone()).await?;
    let mut api_listeners = Vec::new();
    let mut api_runtimes = Vec::new();
    let setup: io::Result<()> = async {
        for (grpc, socket, settings, runtime) in api_sockets {
            api_runtimes.push(runtime.clone());
            let listener = serve_role_bound(socket, runtime, settings, gate.clone(), grpc).await?;
            api_listeners.push((grpc, listener));
        }
        Ok(())
    }
    .await;
    if let Err(error) = setup {
        service.stop_submissions();
        for (_, listener) in api_listeners {
            let _ = listener.shutdown().await;
        }
        for runtime in api_runtimes {
            runtime.shutdown().await;
        }
        let _ = running.shutdown().await;
        if let Err(error) = service.drain().await { eprintln!("{error}"); std::process::exit(1); }
        return Err(error.into());
    }
    start.send_replace(true);
    let address = running.address;
    let output = (|| -> io::Result<()> {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "postgresql={address} preview=single-host")?;
        stdout.flush()?;
        if password.is_none() {
            eprintln!("existing data directory: owner password unchanged");
        }
        writeln!(
            stdout,
            "{}",
            credential_url(&options, address, password.as_deref())
        )?;
        if password.is_some() {
            password_notice(&options);
        }
        for (grpc, listener) in &api_listeners {
            let protocol = if *grpc { "grpc" } else { "http" };
            if *grpc {
                writeln!(stdout, "grpc: {} (h2c)", listener.address)?;
            } else {
                writeln!(
                    stdout,
                    "http: http://{}/v1/sessions (role sign-in)",
                    listener.address
                )?;
            }
            eprintln!(
                "role_api {protocol} listener on {} has TLS disabled; sessions and credentials cross the network in clear",
                listener.address
            );
        }
        if !options.no_api && !options.trust_local {
            eprintln!(
                "hint: over plain HTTP, sign in with SCRAM (plain passwords need TLS), or restart with --trust-local for password-free local access"
            );
        }
        if options.trust_local {
            eprintln!(
                "WARNING: --trust-local lets any local process connect as any role without a password, including Docker containers (they arrive via 127.0.0.1) and port-forwarded peers. Use only on a single-user machine."
            );
        }
        stdout.flush()
    })();
    let control = match output {
        Ok(()) => control().await,
        Err(error) => Err(error),
    };
    service.stop_submissions();
    let mut stopped = Ok(());
    for (_, listener) in api_listeners {
        if let Err(error) = listener.shutdown().await {
            stopped = Err(error);
        }
    }
    for runtime in api_runtimes {
        runtime.shutdown().await;
    }
    let pg_stopped = running.shutdown().await;
    if let Err(error) = service.drain().await { eprintln!("{error}"); std::process::exit(1); }
    control?;
    stopped?;
    pg_stopped?;
    Ok(())
}

/// Trusted local stdin commands, bounded to sixteen bytes. EOF waits for Ctrl-C.
#[derive(Default)]
pub struct ControlReader {
    input: Option<tokio::io::Stdin>,
    signals_only: bool,
}

impl ControlReader {
    /// Disable stdin entirely, including Tokio's blocking stdin worker.
    pub fn with_stdin(enabled: bool) -> Self {
        Self {
            input: None,
            signals_only: !enabled,
        }
    }

    pub async fn next(&mut self) -> io::Result<Option<Vec<u8>>> {
        if self.signals_only {
            #[cfg(unix)]
            {
                let mut terminate = tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate(),
                )?;
                tokio::select! {
                    result = tokio::signal::ctrl_c() => result?,
                    _ = terminate.recv() => {}
                }
            }
            #[cfg(not(unix))]
            tokio::signal::ctrl_c().await?;
            return Ok(None);
        }
        let input = self.input.get_or_insert_with(tokio::io::stdin);
        let mut command = Vec::new();
        let mut overflow = false;
        loop {
            let byte = tokio::select! {
                result = tokio::signal::ctrl_c() => { result?; return Ok(None); }
                result = input.read_u8() => match result {
                    Ok(byte) => byte,
                    Err(_) => { tokio::signal::ctrl_c().await?; return Ok(None); }
                }
            };
            if byte == b'\n' {
                if overflow {
                    return Ok(Some(Vec::new()));
                }
                return Ok(Some(command));
            } else if byte != b'\r' {
                if command.len() < 16 {
                    command.push(byte);
                } else {
                    overflow = true;
                }
            }
        }
    }
}

async fn control() -> io::Result<()> {
    let mut reader = ControlReader::default();
    while let Some(command) = reader.next().await? {
        match command.as_slice() {
            b"shutdown" => break,
            b"reload" => eprintln!("reload rejected; dev mode has no bindings_file"),
            _ => eprintln!("control rejected; expected shutdown"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(args: &[&str]) -> Result<Options, &'static str> {
        Options::parse(args.iter().map(|value| value.to_string()))
    }
    #[test]
    fn defaults_and_explicit_options() {
        let defaults = parse(&[]).unwrap();
        assert_eq!(defaults.port, 5432);
        assert_eq!(defaults.max_connections, 100);
        assert_eq!(
            parse(&["--max-connections", "1"]).unwrap().max_connections,
            1
        );
        assert_eq!(
            parse(&["--max-connections", "128"]).unwrap().max_connections,
            128
        );
        assert_eq!(
            defaults.engine,
            if cfg!(feature = "engine-rocksdb") {
                Engine::V2
            } else {
                Engine::Legacy
            }
        );
        assert_eq!(
            parse(&["--engine", "legacy"]).unwrap().engine,
            Engine::Legacy
        );
        assert_eq!(parse(&["--engine", "v2"]).unwrap().engine, Engine::V2);
        assert_eq!(defaults.owner, "postgres");
        assert_eq!(defaults.database, "postgres");
        assert_eq!(defaults.data, PathBuf::from("giggles-data"));
        assert!(!defaults.trust_local);
        assert!(!defaults.no_api);
        assert!(parse(&["--no-api", "--port", "65535"]).unwrap().no_api);
        assert_eq!(parse(&["--port", "65533"]).unwrap().port, 65533);
        assert!(!defaults.reset_owner_password);
        assert!(
            parse(&["--reset-owner-password"])
                .unwrap()
                .reset_owner_password
        );
        let options = parse(&[
            "--port",
            "0",
            "--owner",
            "_Owner1",
            "--database",
            "Db_2",
            "--data",
            "somewhere",
            "--trust-local",
        ])
        .unwrap();
        assert_eq!(options.port, 0);
        assert_eq!(options.owner, "_Owner1");
        assert!(options.trust_local);
    }
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn v2_service_reopens_and_serves_catalog_and_sql() {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path =
            std::env::temp_dir().join(format!("v2-service-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                login: true,
                superuser: true,
                ..Default::default()
            })
            .unwrap();
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let mut session = service.role_session("owner").unwrap();
        session
            .execute_sql(
                "CREATE TABLE items (id bigint PRIMARY KEY, label varchar(12))",
                &[],
            )
            .unwrap();
        session
            .execute_sql("INSERT INTO items VALUES (1, 'hello')", &[])
            .unwrap();
        assert_eq!(
            session
                .execute_sql("SELECT * FROM items", &[])
                .unwrap()
                .rows
                .len(),
            1
        );
        let read = session.read_catalog().unwrap();
        let tables = read.tables;
        let typmods = read.metadata.typmods;
        assert_eq!(tables.len(), 1);
        assert_eq!(typmods.get(&tables[0].columns[1].id), Some(&16));
        assert_eq!(
            service
                .admin
                .roles()
                .table_owner("items")
                .unwrap()
                .as_deref(),
            Some("owner")
        );
        drop(session);
        drop(service);
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let mut session = service.role_session("owner").unwrap();
        assert_eq!(
            session
                .execute_sql("SELECT * FROM items", &[])
                .unwrap()
                .rows
                .len(),
            1
        );
        drop(session);
        drop(service);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn malformed_cli_is_rejected() {
        for args in [
            vec!["--unknown"],
            vec!["--engine"],
            vec!["--engine", "rocks"],
            vec!["--data"],
            vec!["--port", "65536"],
            vec!["--port", "65534"],
            vec!["--port", "65535"],
            vec!["--port", "-1"],
            vec!["--port", "+1"],
            vec!["--max-connections"],
            vec!["--max-connections", "0"],
            vec!["--max-connections", "129"],
            vec!["--max-connections", "+1"],
            vec!["--owner", "1bad"],
            vec!["--database", "a-b"],
            vec!["--owner", "é"],
            vec!["--data", "--trust-local"],
        ] {
            assert!(parse(&args).is_err(), "{args:?}");
        }
        assert!(valid_name(&"a".repeat(63)));
        assert!(!valid_name(&"a".repeat(64)));
        assert!(!valid_name(""));
    }
    #[test]
    fn uri_password_encoding_and_random_alphabet() {
        assert_eq!(percent_encode("a:@/% é~_-"), "a%3A%40%2F%25%20%C3%A9~_-");
        for _ in 0..8 {
            let value = random_password().unwrap();
            assert_eq!(value.len(), 24);
            assert!(value.bytes().all(|byte| byte.is_ascii_alphanumeric()));
        }
    }
    #[test]
    fn existing_empty_database_bootstraps_with_credentials() {
        let path = std::env::temp_dir().join(format!("empty-{}", random_password().unwrap()));
        std::fs::create_dir_all(&path).unwrap();
        let db = path.join("db");
        let admin = Admin::create(&db, Limits::default()).unwrap();
        assert!(admin.roles().is_empty().unwrap());
        drop(admin);
        let admin = open_existing(&db).unwrap();
        let password = bootstrap_owner(&admin, "postgres").unwrap();
        assert!(password.is_some());
        let options = parse(&[]).unwrap();
        let url = credential_url(&options, ([127, 0, 0, 1], 5432).into(), password.as_deref());
        assert_eq!(
            url,
            format!(
                "postgres://postgres:{}@127.0.0.1:5432/postgres",
                percent_encode(password.as_ref().unwrap())
            )
        );
        assert!(admin.roles().get("postgres").unwrap().is_some());
    }

    #[test]
    fn reset_changes_only_password_and_generation_and_requires_owner() {
        let path = std::env::temp_dir().join(format!("reset-{}", random_password().unwrap()));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db"), Limits::default()).unwrap();
        let roles = admin.roles();
        let before = roles
            .create(NewRole {
                inherit: None,
                name: "owner".into(),
                superuser: false,
                login: false,
                createrole: true,
                password: None,
            })
            .unwrap();
        assert!(reset_owner_password(&admin, "missing").is_err());
        assert_eq!(roles.get("owner").unwrap().unwrap(), before);
        let password = reset_owner_password(&admin, "owner").unwrap();
        assert!(!password.is_empty());
        let mut after = roles.get("owner").unwrap().unwrap();
        assert!(Verifier::parse(after.password.as_ref().unwrap()).is_some());
        assert_eq!(after.generation, before.generation + 1);
        after.password = before.password.clone();
        after.generation = before.generation;
        assert_eq!(after, before);
    }

    #[test]
    fn bootstrap_creates_owner_once_and_preserves_existing_roles() {
        let path = std::env::temp_dir().join(format!(
            "bootstrap-{}-{}",
            std::process::id(),
            random_password().unwrap()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db"), Limits::default()).unwrap();
        let password = bootstrap_owner(&admin, "owner").unwrap().unwrap();
        assert!(!password.is_empty());
        let roles = admin.roles();
        let owner = roles.get("owner").unwrap().unwrap();
        assert!(owner.superuser && owner.login && owner.createrole);
        assert!(Verifier::parse(owner.password.as_ref().unwrap()).is_some());
        assert_eq!(bootstrap_owner(&admin, "owner").unwrap(), None);
        assert_eq!(roles.get("owner").unwrap().unwrap(), owner);
        assert_eq!(bootstrap_owner(&admin, "missing").unwrap(), None);
        assert!(roles.get("missing").unwrap().is_none());
        assert_eq!(roles.list().unwrap().len(), 1 + giggles_core::roles::PREDEFINED.len());
    }
}
