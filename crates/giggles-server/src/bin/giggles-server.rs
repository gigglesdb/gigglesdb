use giggles_core::{Admin, Limits};
use giggles_server::{
    Binding, Running, Service,
    api::{
        ApiLimits, ApiRuntime,
        net::{serve_grpc_bound, serve_http_bound},
    },
    clock::Clock,
    serve_bound, serve_pg_bound, tls,
};
use serde::Deserialize;
use std::{io, io::Read, net::SocketAddr};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::watch};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    database: String,
    native: SocketAddr,
    postgres: SocketAddr,
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
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: giggles-server CONFIG.json")?;
    let bytes = bounded_file(&path)?;
    let c: Config = serde_json::from_slice(&bytes)?;
    if c.connections == 0 || c.connections > 128 {
        return Err("connection limit".into());
    }
    c.api_limits.validate()?;
    let admin = Admin::open(c.database, Limits::default())?;
    let bindings = match &c.bindings_file {
        Some(path) => serde_json::from_slice(&bounded_file(path)?)?,
        None => c.bindings,
    };
    let service = Service::with_limits(
        admin,
        bindings,
        Clock::new(c.skew_seconds, c.max_pause_seconds)?,
        c.service_limits,
    )?;
    let config = tls::server(
        tls::certs(c.certificate)?,
        tls::key(c.private_key)?,
        tls::certs(c.client_ca)?,
    )?;
    // Bind every requested socket before any accept loop can process traffic.
    // Dropping these listeners rolls back a partial bind failure automatically.
    let native_socket = TcpListener::bind(c.native).await?;
    let pg_socket = TcpListener::bind(c.postgres).await?;
    let http_socket = match c.http {
        Some(address) => Some(TcpListener::bind(address).await?),
        None => None,
    };
    let grpc_socket = match c.grpc {
        Some(address) => Some(TcpListener::bind(address).await?),
        None => None,
    };
    let api = ApiRuntime::new(service.clone(), c.api_limits)?;
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
        Ok(())
    }
    .await;
    if let Err(error) = setup {
        drop(start);
        let _ = stop_all(listeners, &api).await;
        return Err(error.into());
    }
    start.send_replace(true);
    println!("{} preview=single-host", addresses.join(" "));
    // Trusted local stdin only; bounded command reader, one reload at a time.
    // CA/server key material is immutable for this process and requires restart.
    let mut input = tokio::io::stdin();
    let mut command = Vec::new();
    let mut overflow = false;
    loop {
        let byte = tokio::select! {
            result = tokio::signal::ctrl_c() => { result?; break; }
            result = input.read_u8() => match result { Ok(byte) => byte, Err(_) => { tokio::signal::ctrl_c().await?; break; } }
        };
        if byte == b'\n' {
            if !overflow && command.as_slice() == b"shutdown" {
                break;
            }
            if !overflow && command.as_slice() == b"reload" {
                if let Some(path) = c.bindings_file.clone() {
                    let service = service.clone();
                    let result = tokio::task::spawn_blocking(
                        move || -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
                            let bindings = serde_json::from_slice(&bounded_file(&path)?)?;
                            Ok(service.reload_bindings(bindings)?)
                        },
                    )
                    .await?;
                    match result {
                        Ok(epoch) => {
                            api.invalidate_bindings();
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
            command.clear();
            overflow = false;
        } else if byte != b'\r' {
            if command.len() < 16 {
                command.push(byte);
            } else {
                overflow = true;
            }
        }
    }
    stop_all(listeners, &api).await?;
    Ok(())
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
