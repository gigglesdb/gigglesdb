//! Agent listeners (GD-0061 R3-205, design-2 sections 1 and 3): the `agent`
//! config section and the agent pgwire accept loop.
//!
//! The agent listener is a port of its own. Its accept loop accepts only
//! certificates in the agent binding map and builds only task sessions
//! (`Service::agent_session`); the ordinary listeners never build one.

use super::AgentBinding;
use crate::{
    Service,
    api::ApiLimits,
    transport::{IO_TIMEOUT, Running},
};
use rustls::ServerConfig;
use serde::Deserialize;
use std::{io, io::Read, net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, oneshot, watch},
    time::timeout,
};
use tokio_rustls::TlsAcceptor;

/// Largest bindings file the server reads.
const MAX_BINDINGS_FILE: u64 = 1024 * 1024;

/// The optional `agent` section of the server config.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    /// Agent pgwire listener (R3-205).
    #[serde(default)]
    pub postgres: Option<SocketAddr>,
    /// Agent HTTP listener (R3-208).
    #[serde(default)]
    pub http: Option<SocketAddr>,
    /// Agent gRPC listener (R3-208).
    #[serde(default)]
    pub grpc: Option<SocketAddr>,
    /// CA for agent client certificates; defaults to the top-level `client_ca`.
    #[serde(default)]
    pub client_ca: Option<String>,
    #[serde(default)]
    pub bindings: Vec<AgentBinding>,
    /// Wins over `bindings` when set, as for the ordinary bindings.
    #[serde(default)]
    pub bindings_file: Option<String>,
    /// Databases an agent startup packet may name.
    #[serde(default = "default_databases")]
    pub databases: Vec<String>,
    #[serde(default = "default_connections")]
    pub connections: usize,
    /// Limits for the agent HTTP and gRPC listeners (R3-208).
    #[serde(default)]
    pub api_limits: ApiLimits,
}

fn default_databases() -> Vec<String> {
    vec!["giggles".into()]
}

fn default_connections() -> usize {
    32
}

impl AgentConfig {
    /// Checks the section on its own. Bindings are validated by the service.
    pub fn validate(&self) -> Result<(), String> {
        if self.postgres.is_none() && self.http.is_none() && self.grpc.is_none() {
            return Err("agent requires at least one of postgres, http, grpc".into());
        }
        if !(1..=128).contains(&self.connections) {
            return Err("agent connections must be 1 to 128".into());
        }
        if self.databases.is_empty() || self.databases.len() > 64 {
            return Err("agent databases must list 1 to 64 names".into());
        }
        for (i, name) in self.databases.iter().enumerate() {
            if name.is_empty() || name.len() > 63 {
                return Err("agent database names must be 1 to 63 bytes".into());
            }
            if self.databases[..i].contains(name) {
                return Err("agent databases must not repeat a name".into());
            }
        }
        if self.client_ca.as_deref().is_some_and(str::is_empty)
            || self.bindings_file.as_deref().is_some_and(str::is_empty)
        {
            return Err("agent paths must not be empty".into());
        }
        self.api_limits
            .validate()
            .map_err(|_| "agent api_limits.sessions must be 1 to 128".to_owned())?;
        Ok(())
    }

    /// The agent bindings: `bindings_file` when set, else the inline list.
    pub fn load_bindings(&self) -> io::Result<Vec<AgentBinding>> {
        let Some(path) = &self.bindings_file else {
            return Ok(self.bindings.clone());
        };
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(MAX_BINDINGS_FILE + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_BINDINGS_FILE {
            return Err(io::Error::other("agent bindings file limit"));
        }
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }
}

/// Starts a pre-bound agent pgwire listener after the common startup gate opens.
///
/// Per connection, in order: SSLRequest; TLS with the agent CA (`tls`); the
/// client certificate's fingerprint must be in the agent map, else the
/// connection is dropped; then `pgwire::run_agent`.
pub async fn serve_agent_pg_bound(
    listener: TcpListener,
    service: Arc<Service>,
    tls: Arc<ServerConfig>,
    connections: usize,
    databases: Vec<String>,
    mut start: watch::Receiver<bool>,
) -> io::Result<Running> {
    if connections == 0 || connections > 128 {
        return Err(io::Error::other("connection limit"));
    }
    let address = listener.local_addr()?;
    let databases: Arc<[String]> = databases.into();
    let (stop, mut rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        while !*start.borrow_and_update() {
            tokio::select! {
                _ = &mut rx => return Ok(()),
                changed = start.changed() => if changed.is_err() { return Ok(()); },
            }
        }
        let sem = Arc::new(Semaphore::new(connections));
        let accept = TlsAcceptor::from(tls);
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut rx => break,
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                incoming = listener.accept() => {
                    let (mut socket, _) = incoming?;
                    crate::transport::no_delay(&socket);
                    let Some(shared) = service.admission.connect() else { continue };
                    let Ok(permit) = sem.clone().try_acquire_owned() else { continue };
                    let accept = accept.clone();
                    let service = service.clone();
                    let databases = databases.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        if crate::pgwire::ssl_request(&mut socket).await.is_err() {
                            eprintln!("protocol=agent_postgres rejected stage=ssl_request reason=TLS_SSLRequest_required");
                            return;
                        }
                        let Ok(Ok(mut stream)) = timeout(IO_TIMEOUT, accept.accept(socket)).await else {
                            eprintln!("protocol=agent_postgres rejected stage=tls reason=handshake_failed_or_timed_out");
                            return;
                        };
                        let Some(cert) = stream.get_ref().1.peer_certificates().and_then(|c| c.first()) else {
                            eprintln!("protocol=agent_postgres rejected stage=tls reason=client_certificate_required");
                            return;
                        };
                        let fingerprint = crate::tls::fingerprint(cert.as_ref());
                        if !service.agent_known(&fingerprint) {
                            eprintln!("protocol=agent_postgres rejected stage=binding reason=client_certificate_not_authorized");
                            return;
                        }
                        if let Err(error) = crate::pgwire::run_agent(
                            &mut stream, &service, &fingerprint, &databases, shared,
                        ).await {
                            eprintln!("protocol=agent_postgres closed stage=wire io_kind={:?}", error.kind());
                        }
                    });
                }
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    });
    Ok(Running::new(address, stop, task))
}
