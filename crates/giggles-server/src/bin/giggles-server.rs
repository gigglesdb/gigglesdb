use giggles_core::{Admin, Limits};
use giggles_server::{Binding, Service, clock::Clock, serve, serve_pg, tls};
use serde::Deserialize;
use std::{io, io::Read, net::SocketAddr};
use tokio::io::AsyncReadExt;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    database: String,
    native: SocketAddr,
    postgres: SocketAddr,
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
    let native = serve(c.native, service.clone(), config.clone(), c.connections).await?;
    let pg = serve_pg(c.postgres, service.clone(), config, c.connections).await?;
    println!(
        "native={} postgres={} preview=single-host",
        native.address, pg.address
    );
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
                        Ok(epoch) => println!("bindings reloaded epoch={epoch}"),
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
    native.shutdown().await?;
    pg.shutdown().await?;
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
