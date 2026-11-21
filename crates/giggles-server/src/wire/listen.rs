//! Listeners for ordinary, agent and role native wire sessions.

use super::{
    auth::RoleWireAuth,
    conn::{WireLimits, serve_connection},
};
use crate::{
    Running,
    api::{ApiRuntime, ListenerKind, error::ApiError},
    tls,
    transport::{IO_TIMEOUT, no_delay},
};
use rustls::ServerConfig;
use std::{io, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, oneshot, watch},
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::TlsAcceptor;

#[cfg(feature = "test-hooks")]
pub use super::conn::ConnectionStats;
#[cfg(feature = "test-hooks")]
use std::{collections::HashMap, net::SocketAddr, sync::Mutex};

#[cfg(feature = "test-hooks")]
#[derive(Default)]
pub(crate) struct WireStats(Mutex<HashMap<SocketAddr, Arc<ConnectionStats>>>);

#[cfg(feature = "test-hooks")]
impl WireStats {
    pub(crate) fn connection(&self, peer: SocketAddr) -> Option<Arc<ConnectionStats>> {
        self.0.lock().unwrap().get(&peer).cloned()
    }

    fn register(&self, peer: SocketAddr) -> ObservedConnection<'_> {
        let stats = Arc::new(ConnectionStats::default());
        self.0.lock().unwrap().insert(peer, stats.clone());
        ObservedConnection {
            map: self,
            peer,
            stats,
        }
    }
}

#[cfg(feature = "test-hooks")]
struct ObservedConnection<'a> {
    map: &'a WireStats,
    peer: SocketAddr,
    stats: Arc<ConnectionStats>,
}

#[cfg(feature = "test-hooks")]
impl Drop for ObservedConnection<'_> {
    fn drop(&mut self) {
        self.map.0.lock().unwrap().remove(&self.peer);
    }
}

/// Serves a pre-bound wire listener once the common startup gate opens.
pub async fn serve_wire_bound(
    listener: TcpListener,
    runtime: Arc<ApiRuntime>,
    config: Arc<ServerConfig>,
    connections: usize,
    mut start: watch::Receiver<bool>,
) -> io::Result<Running> {
    if !(1..=128).contains(&connections) {
        return Err(io::Error::other("connection limit"));
    }
    let address = listener.local_addr()?;
    let mut config = (*config).clone();
    config.alpn_protocols.clear();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        while !*start.borrow_and_update() {
            tokio::select! {
                _ = &mut stopped => return Ok(()),
                changed = start.changed() => if changed.is_err() { return Ok(()); },
            }
        }
        let cap = Arc::new(Semaphore::new(connections));
        let mut sockets = JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                Some(_) = sockets.join_next(), if !sockets.is_empty() => {},
                incoming = listener.accept() => {
                    let (socket, _peer) = incoming?;
                    no_delay(&socket);
                    let Some(shared) = runtime.service().admission.connect() else { continue; };
                    let Ok(local) = cap.clone().try_acquire_owned() else { continue; };
                    let runtime = runtime.clone();
                    let acceptor = acceptor.clone();
                    sockets.spawn(async move {
                        let _local = local;
                        let Ok(Ok(stream)) = timeout(IO_TIMEOUT, acceptor.accept(socket)).await else {
                            return;
                        };
                        let cert = stream.get_ref().1.peer_certificates().and_then(|chain| chain.first());
                        // agent.wire requires a certificate binding. A client
                        // with only an agent bearer credential fails sign-in.
                        let identity = match cert {
                            Some(cert) => runtime.authenticate(&tls::fingerprint(cert.as_ref())),
                            None if runtime.kind() == ListenerKind::Agent => Err(ApiError::Unauthenticated),
                            None => return,
                        };
                        #[cfg(feature = "test-hooks")]
                        let observed = runtime.wire_stats.register(_peer);
                        #[cfg(feature = "test-hooks")]
                        let runtime = runtime.clone();
                        let _ = serve_connection(
                            stream, runtime, identity, WireLimits::default(), shared,
                            #[cfg(feature = "test-hooks")]
                            observed.stats.clone(),
                        ).await;
                    });
                }
            }
        }
        sockets.shutdown().await;
        Ok(())
    });
    Ok(Running::new(address, stop, task))
}

/// Serves a pre-bound role wire listener once the common startup gate opens.
pub async fn serve_role_wire_bound(
    listener: TcpListener,
    runtime: Arc<ApiRuntime>,
    settings: Arc<crate::auth::RoleApiListener>,
    mut start: watch::Receiver<bool>,
) -> io::Result<Running> {
    let connections = settings.connections;
    if !(1..=128).contains(&connections) {
        return Err(io::Error::other("connection limit"));
    }
    let address = listener.local_addr()?;
    let auth = RoleWireAuth::new(&settings)?;
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        while !*start.borrow_and_update() {
            tokio::select! {
                _ = &mut stopped => return Ok(()),
                changed = start.changed() => if changed.is_err() { return Ok(()); },
            }
        }
        let cap = Arc::new(Semaphore::new(connections));
        let mut sockets = JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                Some(_) = sockets.join_next(), if !sockets.is_empty() => {},
                incoming = listener.accept() => {
                    let (socket, peer) = incoming?;
                    no_delay(&socket);
                    let Some(shared) = runtime.service().admission.connect() else { continue; };
                    let Ok(local) = cap.clone().try_acquire_owned() else { continue; };
                    let runtime = runtime.clone();
                    let auth = auth.clone();
                    sockets.spawn(async move {
                        let _local = local;
                        let Ok((stream, identity)) = auth.accept(socket, peer).await else {
                            return;
                        };
                        #[cfg(feature = "test-hooks")]
                        let observed = runtime.wire_stats.register(peer);
                        #[cfg(feature = "test-hooks")]
                        let runtime = runtime.clone();
                        let _ = serve_connection(
                            stream, runtime, Ok(identity), WireLimits::default(), shared,
                            #[cfg(feature = "test-hooks")]
                            observed.stats.clone(),
                        ).await;
                    });
                }
            }
        }
        sockets.shutdown().await;
        Ok(())
    });
    Ok(Running::new(address, stop, task))
}
