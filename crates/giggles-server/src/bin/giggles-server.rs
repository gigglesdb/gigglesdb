use giggles_core::{Admin, Limits};
use giggles_server::{Binding, Service, clock::Clock, serve, serve_pg, tls};
use serde::Deserialize;
use std::{io, net::SocketAddr};
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
    skew_seconds: u64,
    max_pause_seconds: u64,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: giggles-server CONFIG.json")?;
    let bytes = std::fs::read(path)?;
    if bytes.len() > 1024 * 1024 {
        return Err(io::Error::other("config limit").into());
    }
    let c: Config = serde_json::from_slice(&bytes)?;
    let admin = Admin::open(c.database, Limits::default())?;
    let service = Service::new(
        admin,
        c.bindings,
        Clock::new(c.skew_seconds, c.max_pause_seconds)?,
    )?;
    let config = tls::server(
        tls::certs(c.certificate)?,
        tls::key(c.private_key)?,
        tls::certs(c.client_ca)?,
    )?;
    let native = serve(c.native, service.clone(), config.clone(), c.connections).await?;
    let pg = serve_pg(c.postgres, service, config, c.connections).await?;
    println!(
        "native={} postgres={} preview=single-host",
        native.address, pg.address
    );
    tokio::signal::ctrl_c().await?;
    native.shutdown().await?;
    pg.shutdown().await?;
    Ok(())
}
