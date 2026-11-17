use giggles_core::{Admin, Limits};
use giggles_server::{
    Binding, Running, Service,
    agent::{AgentConfig, serve_agent_pg_bound},
    api::{
        ApiLimits, ApiRuntime,
        net::{serve_grpc_bound, serve_http_bound},
    },
    auth::{PostgresqlConfig, serve_postgresql_bound},
    clock::Clock,
    serve_bound, serve_pg_bound, tls,
};
use serde::Deserialize;
use std::{
    io,
    io::{Read, Write},
    net::SocketAddr,
};
use tokio::{net::TcpListener, sync::watch};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    database: String,
    #[serde(default)]
    replication: Option<giggles_server::replication::config::ReplicationConfig>,
    #[serde(default)]
    cluster: Option<giggles_server::cluster::config::ClusterConfig>,
    native: SocketAddr,
    postgres: SocketAddr,
    #[serde(default)]
    postgresql: Option<PostgresqlConfig>,
    #[serde(default)]
    http: Option<SocketAddr>,
    #[serde(default)]
    grpc: Option<SocketAddr>,
    #[serde(default)]
    api_limits: ApiLimits,
    certificate: String,
    private_key: String,
    client_ca: String,
    bindings: Vec<Binding>,
    connections: usize,
    #[serde(default)]
    service_limits: giggles_server::admission::ServiceLimits,
    #[serde(default)]
    bindings_file: Option<String>,
    skew_seconds: u64,
    max_pause_seconds: u64,
    /// GD-0061 R3-205: agent (task) listeners and their binding map.
    #[serde(default)]
    agent: Option<AgentConfig>,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let Some(path) = args.next() else {
        eprintln!("{}", giggles_server::dev::USAGE);
        std::process::exit(2);
    };
    if path == "replica" {
        std::process::exit(replica_command(args).await);
    }
    if path == "cluster" {
        std::process::exit(cluster_command(args).await);
    }
    if path == "--dev" {
        let options = giggles_server::dev::Options::parse(args).unwrap_or_else(|usage| {
            eprintln!("{usage}");
            std::process::exit(2);
        });
        return giggles_server::dev::run(options).await;
    }
    if path.starts_with('-') || args.next().is_some() {
        eprintln!("{}", giggles_server::dev::USAGE);
        std::process::exit(2);
    }
    let bytes = bounded_file(&path)?;
    let c: Config = serde_json::from_slice(&bytes)?;
    validate_agent(&c)?;
    let cluster = match &c.cluster {
        Some(cluster) => Some(cluster_validate(&c, cluster)?),
        None => None,
    };
    if c.replication.is_some() {
        #[cfg(not(feature = "engine-rocksdb"))]
        return Err("replication requires the engine-rocksdb feature".into());
        #[cfg(feature = "engine-rocksdb")]
        return run_replicated(c).await;
    }
    if c.connections == 0 || c.connections > 128 {
        return Err("connection limit".into());
    }
    c.api_limits.validate()?;
    let mut postgresql = c
        .postgresql
        .as_ref()
        .map(PostgresqlConfig::build)
        .transpose()?
        .map(std::sync::Arc::new);
    #[cfg(feature = "engine-rocksdb")]
    let mut cluster_runtime = None;
    #[cfg_attr(not(feature = "engine-rocksdb"), allow(unused_mut))]
    let mut preview = "preview=single-host".to_string();
    let admin = match cluster {
        #[cfg(feature = "engine-rocksdb")]
        Some(v) => {
            let database = c.database.clone();
            let runtime = tokio::task::spawn_blocking(move || {
                giggles_server::cluster::runtime::ClusterRuntime::start(
                    &v,
                    std::path::Path::new(&database),
                )
            })
            .await??;
            preview = format!("preview=cluster node={}", runtime.node_id());
            let admin = runtime.admin().clone();
            cluster_runtime = Some(runtime);
            admin
        }
        #[cfg(not(feature = "engine-rocksdb"))]
        Some(_) => unreachable!("cluster_validate refuses without engine-rocksdb"),
        None => Admin::open(&c.database, Limits::default())?,
    };
    let bindings = match &c.bindings_file {
        Some(path) => serde_json::from_slice(&bounded_file(path)?)?,
        None => c.bindings.clone(),
    };
    let agent_bindings = match &c.agent {
        Some(agent) => agent.load_bindings()?,
        None => Vec::new(),
    };
    let service = Service::with_agent(
        admin,
        bindings,
        agent_bindings,
        Clock::new(c.skew_seconds, c.max_pause_seconds)?,
        c.service_limits,
    )?;
    let mut config = tls::server(
        tls::certs(&c.certificate)?,
        tls::key(&c.private_key)?,
        tls::certs(&c.client_ca)?,
    )?;
    let certificate_reload = tls::ReloadableCertificate::install(&mut config);
    // The agent listener has its own TLS config: the same server certificate,
    // and the agent CA for client certificates.
    let mut agent_tls = match &c.agent {
        Some(agent) => Some(tls::server(
            tls::certs(&c.certificate)?,
            tls::key(&c.private_key)?,
            tls::certs(agent.client_ca.as_deref().unwrap_or(&c.client_ca))?,
        )?),
        None => None,
    };
    let agent_certificate_reload = agent_tls.as_mut().map(tls::ReloadableCertificate::install);
    let postgresql_reload = postgresql.as_mut().and_then(|listener| {
        std::sync::Arc::get_mut(listener).and_then(|listener| {
            listener
                .tls_config
                .as_mut()
                .map(tls::ReloadableCertificate::install)
        })
    });
    let StartedListeners {
        listeners,
        api,
        agent_api,
        addresses,
    } = start_listeners(&c, &service, &config, postgresql, agent_tls, false).await?;
    println!("{} {preview}", addresses.join(" "));
    io::stdout().flush()?;
    // Trusted local stdin only; bounded command reader, one reload at a time.
    // Certificate reload uses the same trusted local trigger as binding reload.
    let mut reader = giggles_server::dev::ControlReader::default();
    while let Some(command) = reader.next().await? {
        if command.as_slice() == b"shutdown" {
            break;
        }
        if command.as_slice() == b"reload" {
            match certificate_reload.reload(&c.certificate, &c.private_key) {
                Ok(()) => {}
                Err(error) => eprintln!("TLS reload rejected; prior certificate retained: {error}"),
            }
            if let Some(resolver) = &agent_certificate_reload
                && let Err(error) = resolver.reload(&c.certificate, &c.private_key)
            {
                eprintln!("agent TLS reload rejected; prior certificate retained: {error}");
            }
            if let (Some(resolver), Some(pg)) = (&postgresql_reload, &c.postgresql) {
                match resolver.reload(
                    pg.certificate.as_ref().unwrap(),
                    pg.private_key.as_ref().unwrap(),
                ) {
                    Ok(()) => {}
                    Err(error) => eprintln!(
                        "PostgreSQL TLS reload rejected; prior certificate retained: {error}"
                    ),
                }
            }
            let agent_file = c.agent.as_ref().and_then(|a| a.bindings_file.as_ref());
            if c.bindings_file.is_some() || agent_file.is_some() {
                // Both maps are read and swapped together; a map without a
                // file keeps its inline bindings.
                let (path, inline, agent) =
                    (c.bindings_file.clone(), c.bindings.clone(), c.agent.clone());
                let service = service.clone();
                let result = tokio::task::spawn_blocking(
                    move || -> Result<(u64, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
                        let bindings = match path {
                            Some(path) => serde_json::from_slice(&bounded_file(&path)?)?,
                            None => inline,
                        };
                        let agent = match &agent {
                            Some(agent) => agent.load_bindings()?,
                            None => Vec::new(),
                        };
                        let (epoch, _) = service.reload_all_bindings(bindings, agent)?;
                        Ok((epoch, service.stale_agent_bindings()))
                    },
                )
                .await?;
                match result {
                    Ok((epoch, stale)) => {
                        api.invalidate_bindings();
                        if let Some(agent_api) = &agent_api {
                            agent_api.invalidate_bindings();
                        }
                        for fingerprint in stale {
                            eprintln!("agent_binding_stale fingerprint={fingerprint}");
                        }
                        println!("bindings reloaded epoch={epoch}");
                    }
                    Err(_) => eprintln!("reload rejected; prior bindings retained"),
                }
            } else {
                eprintln!("reload rejected; bindings_file is not configured");
            }
        } else {
            eprintln!("control rejected; expected reload");
        }
    }
    let stopped = stop_all(listeners, &api).await;
    if let Some(agent_api) = &agent_api {
        agent_api.shutdown().await;
    }
    stopped?;
    drop(service);
    #[cfg(feature = "engine-rocksdb")]
    if let Some(runtime) = cluster_runtime {
        tokio::task::spawn_blocking(move || runtime.stop()).await?;
    }
    Ok(())
}

/// The validated `cluster` section, or why this configuration cannot run in cluster mode.
fn cluster_validate(
    c: &Config,
    cluster: &giggles_server::cluster::config::ClusterConfig,
) -> Result<giggles_server::cluster::config::ClusterValidated, String> {
    use std::path::Path;
    if c.replication.is_some() {
        return Err("cluster: cannot be combined with replication".into());
    }
    if !cfg!(feature = "engine-rocksdb") {
        return Err("cluster requires the engine-rocksdb feature".into());
    }
    cluster.validate(
        Path::new(&c.database),
        Path::new(&c.certificate),
        Path::new(&c.private_key),
        Path::new(&c.client_ca),
    )
}

#[cfg(not(feature = "engine-rocksdb"))]
async fn cluster_command(_args: impl Iterator<Item = String>) -> i32 {
    eprintln!("cluster requires the engine-rocksdb feature");
    2
}

/// `cluster status <config.json> [--json]`: every member's ranges, leaders and load, asked
/// with this node's certificate. Exit 0 if a member answered, 1 if none, 2 on bad usage.
#[cfg(feature = "engine-rocksdb")]
async fn cluster_command(args: impl Iterator<Item = String>) -> i32 {
    use giggles_server::cluster::{cli, runtime::transport_config};
    let command = match cli::Command::parse(args) {
        Ok(command) => command,
        Err(usage) => {
            eprintln!("{usage}");
            return 2;
        }
    };
    let setup = || -> Result<_, Box<dyn std::error::Error>> {
        let c: Config = serde_json::from_slice(&bounded_file(&command.config)?)?;
        let cluster = c.cluster.as_ref().ok_or("cluster section is required")?;
        let v = cluster_validate(&c, cluster)?;
        Ok((transport_config(&v)?, v.members))
    };
    let (transport, members) = match setup() {
        Ok(value) => value,
        Err(error) => {
            eprintln!("{error}\n{}", cli::USAGE);
            return 2;
        }
    };
    let asked = tokio::task::spawn_blocking(move || {
        giggles_replication::cluster_node::query_status(&transport, &members, cli::TIMEOUT)
    })
    .await;
    let answers = match asked {
        Ok(Ok(answers)) => answers,
        Ok(Err(error)) => {
            eprintln!("transport error: {error}");
            return 1;
        }
        Err(error) => {
            eprintln!("transport error: {error}");
            return 1;
        }
    };
    let summary = cli::summarize(&answers);
    if command.json {
        match serde_json::to_string_pretty(&summary) {
            Ok(json) => println!("{json}"),
            Err(error) => {
                eprintln!("{error}");
                return 1;
            }
        }
    } else {
        println!("{}", cli::text(&summary));
    }
    summary.exit_code()
}

#[cfg(not(feature = "engine-rocksdb"))]
async fn replica_command(_args: impl Iterator<Item = String>) -> i32 {
    eprintln!("replication requires the engine-rocksdb feature");
    2
}

#[cfg(feature = "engine-rocksdb")]
async fn replica_command(args: impl Iterator<Item = String>) -> i32 {
    use giggles_server::replication::{cli, runtime::transport_config};
    use std::{path::Path, time::Duration};
    let command = match cli::Command::parse(args) {
        Ok(command) => command,
        Err(usage) => {
            eprintln!("{usage}");
            return 2;
        }
    };
    let setup = || -> Result<_, Box<dyn std::error::Error>> {
        let c: Config = serde_json::from_slice(&bounded_file(&command.config)?)?;
        let v = c
            .replication
            .as_ref()
            .ok_or("replication section is required")?
            .validate(
                Path::new(&c.database),
                Path::new(&c.certificate),
                Path::new(&c.private_key),
                Path::new(&c.client_ca),
            )?;
        Ok((transport_config(&v)?, v.node_id))
    };
    let (transport, node_id) = match setup() {
        Ok(value) => value,
        Err(error) => {
            eprintln!("{error}\n{}", cli::USAGE);
            return 2;
        }
    };
    // admin::call owns a synchronous Tokio runtime.
    match tokio::task::spawn_blocking(move || {
        giggles_replication::admin::call(
            &transport,
            node_id,
            &command.request,
            command.timeout + Duration::from_secs(30),
        )
    })
    .await
    {
        Ok(Ok(response)) => {
            let (code, text) = cli::output(response);
            if code == 0 {
                println!("{text}");
            } else {
                eprintln!("{text}");
            }
            code
        }
        Ok(Err(error)) => {
            eprintln!("transport error: {error}");
            4
        }
        Err(error) => {
            eprintln!("transport error: {error}");
            4
        }
    }
}

#[cfg(feature = "engine-rocksdb")]
async fn run_replicated(c: Config) -> Result<(), Box<dyn std::error::Error>> {
    use giggles_replication::role::Serving;
    use giggles_server::replication::runtime::Replicated;
    use std::{path::Path, sync::Arc, time::Duration};

    if !(1..=128).contains(&c.connections) {
        return Err("connection limit".into());
    }
    c.api_limits.validate()?;
    let v = c.replication.as_ref().unwrap().validate(
        Path::new(&c.database),
        Path::new(&c.certificate),
        Path::new(&c.private_key),
        Path::new(&c.client_ca),
    )?;
    let owner = v.owner.clone();
    let mut postgresql = c
        .postgresql
        .as_ref()
        .map(PostgresqlConfig::build)
        .transpose()?
        .map(Arc::new);
    let mut config = tls::server(
        tls::certs(&c.certificate)?,
        tls::key(&c.private_key)?,
        tls::certs(&c.client_ca)?,
    )?;
    let certificate_reload = tls::ReloadableCertificate::install(&mut config);
    let postgresql_reload = postgresql
        .as_mut()
        .and_then(Arc::get_mut)
        .and_then(|listener| {
            listener
                .tls_config
                .as_mut()
                .map(tls::ReloadableCertificate::install)
        });
    let database = c.database.clone();
    let repl =
        tokio::task::spawn_blocking(move || Replicated::start(&v, Path::new(&database))).await??;
    let mut repl = Some(repl);
    let (tx, mut commands) = tokio::sync::mpsc::channel(1);
    let reader = tokio::spawn(async move {
        let mut reader = giggles_server::dev::ControlReader::default();
        loop {
            match reader.next().await {
                Ok(Some(command)) => {
                    if tx.send(Ok(command)).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    break;
                }
            }
        }
    });
    let mut listeners = Vec::new();
    let mut api = None;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        'roles: loop {
            let seen = repl.as_ref().unwrap().current();
            let (service, setup_failed) = match setup_role(
                &c, &seen, repl.as_ref().unwrap(), &owner, &config, postgresql.clone(),
            ).await {
                Ok((started, started_api, service, addresses, role)) => {
                    listeners = started;
                    api = started_api;
                    println!("{} preview=replicated role={role} node={}", addresses.join(" "), repl.as_ref().unwrap().node_id());
                    io::stdout().flush()?;
                    (service, false)
                }
                Err(error) => {
                    let role = match &seen {
                        Serving::Leader { .. } => "leader",
                        Serving::Follower { .. } => "follower",
                    };
                    eprintln!("replication: {role} setup failed: {error}");
                    (None, true)
                }
            };
            loop {
                let previous = seen.clone();
                let runtime = repl.take().unwrap();
                let mut waiting = tokio::task::spawn_blocking(move || {
                    let now = runtime.wait_change(&previous, Duration::from_secs(1));
                    (runtime, now)
                });
                let command = tokio::select! {
                    result = &mut waiting => {
                        let (returned, now) = result?;
                        repl = Some(returned);
                        let changed = giggles_server::replication::runtime::serving_changed(&seen, &now);
                        if changed || setup_failed { break; }
                        continue;
                    }
                    command = commands.recv() => command,
                };
                repl = Some(waiting.await?.0);
                let Some(command) = command else { break 'roles; };
                let command = command?;
                match command.as_slice() {
                    b"shutdown" => break 'roles,
                    b"reload" => {
                        if let Err(error) = certificate_reload.reload(&c.certificate, &c.private_key) {
                            eprintln!("TLS reload rejected; prior certificate retained: {error}");
                        }
                        if let (Some(resolver), Some(pg)) = (&postgresql_reload, &c.postgresql)
                            && let Err(error) = resolver.reload(pg.certificate.as_ref().unwrap(), pg.private_key.as_ref().unwrap()) {
                            eprintln!("PostgreSQL TLS reload rejected; prior certificate retained: {error}");
                        }
                        if let Some(service) = &service {
                            if let Some(path) = c.bindings_file.clone() {
                                let service = service.clone();
                                let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
                                    let bindings = serde_json::from_slice(&bounded_file(&path).map_err(|e| e.to_string())?)
                                        .map_err(|e| e.to_string())?;
                                    service.reload_bindings(bindings).map_err(|e| e.to_string())
                                }).await?;
                                match result {
                                    Ok(epoch) => {
                                        api.as_ref().unwrap().invalidate_bindings();
                                        println!("bindings reloaded epoch={epoch}");
                                    }
                                    Err(_) => eprintln!("reload rejected; prior bindings retained"),
                                }
                            } else { eprintln!("reload rejected; bindings_file is not configured"); }
                        } else { eprintln!("reload rejected; not leader"); }
                    }
                    _ => eprintln!("control rejected; expected reload"),
                }
            }
            stop_role(std::mem::take(&mut listeners), api.take()).await?;
            drop(service);
            drop(seen);
        }
        Ok(())
    }.await;
    reader.abort();
    let _ = reader.await;
    let stopped = stop_role(listeners, api).await;
    tokio::task::spawn_blocking(move || repl.unwrap().stop()).await?;
    result?;
    stopped?;
    Ok(())
}

#[cfg(feature = "engine-rocksdb")]
async fn setup_role(
    c: &Config,
    seen: &giggles_replication::role::Serving<giggles_replication::core_store::ReplicatedEngine>,
    repl: &giggles_server::replication::runtime::Replicated,
    owner: &Option<String>,
    config: &std::sync::Arc<rustls::ServerConfig>,
    postgresql: Option<std::sync::Arc<giggles_server::auth::PostgresListener>>,
) -> Result<
    (
        Vec<Running>,
        Option<std::sync::Arc<ApiRuntime>>,
        Option<std::sync::Arc<Service>>,
        Vec<String>,
        String,
    ),
    Box<dyn std::error::Error>,
> {
    use giggles_replication::role::Serving;
    use giggles_server::replication::refuse::serve_refusal_bound;

    let mut listeners = Vec::new();
    let mut api = None;
    let mut service = None;
    let mut addresses = Vec::new();
    let setup: Result<String, Box<dyn std::error::Error>> = async {
        let role = match seen {
            Serving::Leader { term, engine } => {
                let bootstrap_engine = engine.clone();
                let owner = owner.clone();
                let admin = tokio::task::spawn_blocking(move || -> Result<_, String> {
                    if let Some(owner) = owner {
                        let roles = bootstrap_engine.admin().roles();
                        if roles.is_empty().map_err(|e| e.to_string())? {
                            let password = std::env::var("GIGGLES_OWNER_PASSWORD")
                                .ok().filter(|p| !p.is_empty())
                                .ok_or("GIGGLES_OWNER_PASSWORD is required to bootstrap replication.owner")?;
                            roles.create(giggles_core::roles::NewRole {
                                name: owner, superuser: true, login: true, createrole: true,
                                password: Some(giggles_server::dev::password_verifier(&password)
                                    .map_err(|e| e.to_string())?),
                            }).map_err(|e| e.to_string())?;
                        }
                    }
                    Ok(bootstrap_engine.admin().clone())
                }).await??;
                let bindings = match &c.bindings_file {
                    Some(path) => serde_json::from_slice(&bounded_file(path)?)?,
                    None => c.bindings.clone(),
                };
                let leader = Service::with_limits(admin, bindings,
                    Clock::new(c.skew_seconds, c.max_pause_seconds)?, c.service_limits)?;
                leader.set_statement_gate(repl.statement_gate(engine))
                    .map_err(|_| "statement gate already set")?;
                let started = start_listeners(c, &leader, config, postgresql.clone(), None, true).await?;
                listeners = started.listeners;
                api = Some(started.api);
                addresses = started.addresses;
                service = Some(leader);
                format!("leader term={term}")
            }
            Serving::Follower { .. } => {
                // Bind both sockets before starting either refusal accept loop.
                let pg = bind_listener(c.postgres, true).await?;
                let catalog = match &c.postgresql {
                    Some(pg) => Some(bind_listener(pg.listen, true).await?),
                    None => None,
                };
                let pg = serve_refusal_bound(pg, Some(config.clone()), repl.refusal_hint(), c.connections).await?;
                addresses.push(format!("postgres={}", pg.address));
                listeners.push(pg);
                if let Some(socket) = catalog {
                    let pg = serve_refusal_bound(socket,
                        postgresql.as_ref().and_then(|pg| pg.tls_config.clone()),
                        repl.refusal_hint(), c.connections).await?;
                    addresses.push(format!("postgresql={}", pg.address));
                    listeners.push(pg);
                }
                "follower".into()
            }
        };
        Ok(role)
    }.await;
    match setup {
        Ok(role) => Ok((listeners, api, service, addresses, role)),
        Err(error) => {
            if let Err(cleanup) = stop_role(listeners, api).await {
                eprintln!("replication: setup cleanup failed: {cleanup}");
            }
            Err(error)
        }
    }
}

#[cfg(feature = "engine-rocksdb")]
async fn stop_role(
    listeners: Vec<Running>,
    api: Option<std::sync::Arc<ApiRuntime>>,
) -> io::Result<()> {
    if let Some(api) = api {
        return stop_all(listeners, &api).await;
    }
    let mut tasks = tokio::task::JoinSet::new();
    for mut listener in listeners {
        listener.request_shutdown();
        tasks.spawn(listener.shutdown());
    }
    let mut failure = None;
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result.unwrap_or_else(|e| Err(io::Error::other(e))) {
            failure.get_or_insert(error);
        }
    }
    failure.map_or(Ok(()), Err)
}

struct StartedListeners {
    listeners: Vec<Running>,
    api: std::sync::Arc<ApiRuntime>,
    /// GD-0061 R3-208: the agent HTTP/gRPC runtime, with its own registry.
    agent_api: Option<std::sync::Arc<ApiRuntime>>,
    addresses: Vec<String>,
}

async fn start_listeners(
    c: &Config,
    service: &std::sync::Arc<Service>,
    config: &std::sync::Arc<rustls::ServerConfig>,
    postgresql: Option<std::sync::Arc<giggles_server::auth::PostgresListener>>,
    agent_tls: Option<std::sync::Arc<rustls::ServerConfig>>,
    retry: bool,
) -> Result<StartedListeners, Box<dyn std::error::Error>> {
    // Bind every requested socket before any accept loop can process traffic.
    // Dropping these listeners rolls back a partial bind failure automatically.
    let native_socket = bind_listener(c.native, retry).await?;
    let pg_socket = bind_listener(c.postgres, retry).await?;
    let postgresql_socket = match &c.postgresql {
        Some(config) => Some(bind_listener(config.listen, retry).await?),
        None => None,
    };
    let http_socket = match c.http {
        Some(address) => Some(bind_listener(address, retry).await?),
        None => None,
    };
    let grpc_socket = match c.grpc {
        Some(address) => Some(bind_listener(address, retry).await?),
        None => None,
    };
    let agent_pg_socket = match (c.agent.as_ref().and_then(|a| a.postgres), &agent_tls) {
        (Some(address), Some(_)) => Some(bind_listener(address, retry).await?),
        _ => None,
    };
    let agent_http_socket = match (c.agent.as_ref().and_then(|a| a.http), &agent_tls) {
        (Some(address), Some(_)) => Some(bind_listener(address, retry).await?),
        _ => None,
    };
    let agent_grpc_socket = match (c.agent.as_ref().and_then(|a| a.grpc), &agent_tls) {
        (Some(address), Some(_)) => Some(bind_listener(address, retry).await?),
        _ => None,
    };
    let api = ApiRuntime::new(service.clone(), c.api_limits)?;
    // The agent listeners get a runtime of their own: a separate registry, so
    // a session token from one runtime is never valid on the other.
    let agent_api = match (&c.agent, &agent_tls) {
        (Some(agent), Some(_)) if agent.http.is_some() || agent.grpc.is_some() => {
            Some(ApiRuntime::new_agent(service.clone(), agent.api_limits)?)
        }
        _ => None,
    };
    let (start, gate) = watch::channel(false);
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    let setup: io::Result<()> = async {
        let native = serve_bound(
            native_socket,
            service.clone(),
            config.clone(),
            c.connections,
            gate.clone(),
        )
        .await?;
        addresses.push(format!("native={}", native.address));
        listeners.push(native);
        let pg = serve_pg_bound(
            pg_socket,
            service.clone(),
            config.clone(),
            c.connections,
            gate.clone(),
        )
        .await?;
        addresses.push(format!("postgres={}", pg.address));
        listeners.push(pg);
        if let (Some(socket), Some(config)) = (postgresql_socket, postgresql) {
            let postgresql =
                serve_postgresql_bound(socket, service.clone(), config, gate.clone()).await?;
            addresses.push(format!("postgresql={}", postgresql.address));
            listeners.push(postgresql);
        }
        if let Some(socket) = http_socket {
            let http = serve_http_bound(
                socket,
                api.clone(),
                config.clone(),
                c.connections,
                gate.clone(),
            )
            .await?;
            addresses.push(format!("http={}", http.address));
            listeners.push(http);
        }
        if let Some(socket) = grpc_socket {
            let grpc = serve_grpc_bound(
                socket,
                api.clone(),
                config.clone(),
                c.connections,
                gate.clone(),
            )
            .await?;
            addresses.push(format!("grpc={}", grpc.address));
            listeners.push(grpc);
        }
        if let (Some(runtime), Some(tls), Some(agent)) = (&agent_api, &agent_tls, &c.agent) {
            if let Some(socket) = agent_http_socket {
                let http = serve_http_bound(
                    socket,
                    runtime.clone(),
                    tls.clone(),
                    agent.connections,
                    gate.clone(),
                )
                .await?;
                addresses.push(format!("agent_http={}", http.address));
                listeners.push(http);
            }
            if let Some(socket) = agent_grpc_socket {
                let grpc = serve_grpc_bound(
                    socket,
                    runtime.clone(),
                    tls.clone(),
                    agent.connections,
                    gate.clone(),
                )
                .await?;
                addresses.push(format!("agent_grpc={}", grpc.address));
                listeners.push(grpc);
            }
        }
        if let (Some(socket), Some(tls), Some(agent)) = (agent_pg_socket, agent_tls, &c.agent) {
            let agent_pg = serve_agent_pg_bound(
                socket,
                service.clone(),
                tls,
                agent.connections,
                agent.databases.clone(),
                gate.clone(),
            )
            .await?;
            addresses.push(format!("agent_postgres={}", agent_pg.address));
            listeners.push(agent_pg);
        }
        Ok(())
    }
    .await;
    if let Err(error) = setup {
        drop(start);
        let _ = stop_all(listeners, &api).await;
        if let Some(agent_api) = &agent_api {
            agent_api.shutdown().await;
        }
        return Err(error.into());
    }
    start.send_replace(true);
    Ok(StartedListeners {
        listeners,
        api,
        agent_api,
        addresses,
    })
}

async fn bind_listener(address: SocketAddr, retry: bool) -> io::Result<TcpListener> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match TcpListener::bind(address).await {
            Ok(listener) => return Ok(listener),
            Err(_) if retry && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep_until(
                    (tokio::time::Instant::now() + std::time::Duration::from_millis(100))
                        .min(deadline),
                )
                .await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn stop_all(listeners: Vec<Running>, api: &ApiRuntime) -> io::Result<()> {
    let mut tasks = tokio::task::JoinSet::new();
    for mut listener in listeners {
        listener.request_shutdown();
        tasks.spawn(listener.shutdown());
    }
    api.shutdown().await;
    let mut failure = None;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                failure.get_or_insert(error);
            }
            Err(error) => {
                failure.get_or_insert(io::Error::other(error));
            }
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// The `agent` section on its own, plus what R3 does not serve yet.
fn validate_agent(c: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let Some(agent) = &c.agent else {
        return Ok(());
    };
    agent.validate()?;
    if c.replication.is_some() {
        // D13: replicated databases refuse task statements in R3.
        return Err("agent listeners are not available with replication".into());
    }
    Ok(())
}

fn bounded_file(path: &str) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1024 * 1024 {
        return Err(io::Error::other("config limit"));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_listener_is_optional_and_explicit() {
        let mut json = serde_json::json!({
            "database": "db", "native": "127.0.0.1:0", "postgres": "127.0.0.1:0",
            "certificate": "server.pem", "private_key": "server.key", "client_ca": "ca.pem",
            "bindings": [], "connections": 32, "skew_seconds": 0, "max_pause_seconds": 3600
        });
        assert!(
            serde_json::from_value::<Config>(json.clone())
                .unwrap()
                .postgresql
                .is_none()
        );
        json["postgresql"] = serde_json::json!({"listen": "127.0.0.1:0", "tls": "disabled"});
        let config = serde_json::from_value::<Config>(json.clone())
            .unwrap()
            .postgresql
            .unwrap();
        assert_eq!(config.listen.port(), 0);
        assert!(config.build().is_ok());
        json["postgresql"]["typo"] = true.into();
        assert!(serde_json::from_value::<Config>(json).is_err());
    }

    #[test]
    fn agent_section_is_optional_strict_and_validated() {
        let mut json = serde_json::json!({
            "database": "db", "native": "127.0.0.1:0", "postgres": "127.0.0.1:0",
            "certificate": "server.pem", "private_key": "server.key", "client_ca": "ca.pem",
            "bindings": [], "connections": 32, "skew_seconds": 0, "max_pause_seconds": 3600
        });
        let config = serde_json::from_value::<Config>(json.clone()).unwrap();
        assert!(config.agent.is_none());
        assert!(validate_agent(&config).is_ok());
        json["agent"] = serde_json::json!({});
        assert!(validate_agent(&serde_json::from_value::<Config>(json.clone()).unwrap()).is_err());
        json["agent"] = serde_json::json!({"postgres": "127.0.0.1:0"});
        let config = serde_json::from_value::<Config>(json.clone()).unwrap();
        assert!(validate_agent(&config).is_ok());
        assert_eq!(config.agent.unwrap().databases, ["giggles"]);
        json["agent"]["http"] = "127.0.0.1:0".into();
        json["agent"]["grpc"] = "127.0.0.1:0".into();
        json["agent"]["api_limits"] = serde_json::json!({"sessions": 4});
        let config = serde_json::from_value::<Config>(json.clone()).unwrap();
        assert!(validate_agent(&config).is_ok());
        assert_eq!(config.agent.unwrap().api_limits.sessions, 4);
        json["agent"]["api_limits"] = serde_json::json!({"sessions": 0});
        assert!(validate_agent(&serde_json::from_value::<Config>(json.clone()).unwrap()).is_err());
        json["agent"] = serde_json::json!({"postgres": "127.0.0.1:0", "typo": 1});
        assert!(serde_json::from_value::<Config>(json).is_err());
    }

    #[test]
    fn cluster_section_is_optional_validated_and_exclusive() {
        let mut json = serde_json::json!({
            "database": "db", "native": "127.0.0.1:0", "postgres": "127.0.0.1:0",
            "certificate": "server.pem", "private_key": "server.key", "client_ca": "ca.pem",
            "bindings": [], "connections": 32, "skew_seconds": 0, "max_pause_seconds": 3600
        });
        let c = serde_json::from_value::<Config>(json.clone()).unwrap();
        assert!(c.cluster.is_none() && c.replication.is_none());
        json["cluster"] = serde_json::json!({
            "node_id": 1, "cluster": "0123456789abcdef0123456789abcdef",
            "listen": "127.0.0.1:7101", "log_dir": "cluster-log"
        });
        let c = serde_json::from_value::<Config>(json.clone()).unwrap();
        match cluster_validate(&c, c.cluster.as_ref().unwrap()) {
            Ok(v) => assert_eq!((v.node_id, v.bootstrap_node()), (1, 1)),
            Err(error) => assert_eq!(error, "cluster requires the engine-rocksdb feature"),
        }
        json["cluster"]["node_id"] = 0.into();
        let c = serde_json::from_value::<Config>(json.clone()).unwrap();
        if cfg!(feature = "engine-rocksdb") {
            let error = cluster_validate(&c, c.cluster.as_ref().unwrap()).unwrap_err();
            assert!(error.starts_with("cluster.node_id:"), "{error}");
        }
        json["replication"] = serde_json::json!({
            "node_id": 1, "cluster": "0123456789abcdef0123456789abcdef",
            "listen": "127.0.0.1:7201", "log_dir": "replication-log", "peers": []
        });
        let c = serde_json::from_value::<Config>(json.clone()).unwrap();
        assert_eq!(
            cluster_validate(&c, c.cluster.as_ref().unwrap()).unwrap_err(),
            "cluster: cannot be combined with replication"
        );
        json["cluster"]["typo"] = true.into();
        assert!(serde_json::from_value::<Config>(json).is_err());
    }
}
