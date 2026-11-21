use crate::{Service, tls};
use rustls::ServerConfig;
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, oneshot, watch},
    task::JoinHandle,
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
pub const MAX_FRAME: usize = 256 * 1024;
pub const MAX_RESPONSE: usize = 8 * 1024 * 1024;
pub const IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Turns off Nagle on a server or client TCP socket. Replies leave as several
/// small writes (pgwire message by message, TLS records, frame header then
/// body); with Nagle on, each one after the first waits for the peer's delayed
/// ACK, about 40 ms on Linux. Best effort: a failure only costs latency.
pub fn no_delay(socket: &TcpStream) {
    let _ = socket.set_nodelay(true);
}
pub struct Running {
    pub address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<io::Result<()>>,
}
impl Running {
    pub(crate) fn new(
        address: SocketAddr,
        stop: oneshot::Sender<()>,
        task: JoinHandle<io::Result<()>>,
    ) -> Self {
        Self {
            address,
            stop: Some(stop),
            task,
        }
    }

    /// Signal every listener before awaiting any shutdown work.
    pub fn request_shutdown(&mut self) {
        if let Some(s) = self.stop.take() {
            let _ = s.send(());
        }
    }

    pub async fn shutdown(mut self) -> io::Result<()> {
        self.request_shutdown();
        self.task.await.map_err(io::Error::other)?
    }
}
/// Connections are admitted before TLS allocation. Shutdown aborts socket tasks;
/// synchronous core calls already started can finish their atomic decision.
pub async fn serve_pg(
    address: SocketAddr,
    service: Arc<Service>,
    config: Arc<ServerConfig>,
    connections: usize,
) -> io::Result<Running> {
    if connections == 0 || connections > 128 {
        return Err(io::Error::other("connection limit"));
    }
    let listener = TcpListener::bind(address).await?;
    let (_, start) = watch::channel(true);
    serve_pg_bound(listener, service, config, connections, start).await
}

/// Starts a pre-bound PostgreSQL listener after the common startup gate opens.
pub async fn serve_pg_bound(
    listener: TcpListener,
    service: Arc<Service>,
    config: Arc<ServerConfig>,
    connections: usize,
    mut start: watch::Receiver<bool>,
) -> io::Result<Running> {
    if connections == 0 || connections > 128 {
        return Err(io::Error::other("connection limit"));
    }
    let address = listener.local_addr()?;
    let (stop, mut rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        while !*start.borrow_and_update() {
            tokio::select! {
                _ = &mut rx => return Ok(()),
                changed = start.changed() => if changed.is_err() { return Ok(()); },
            }
        }
        let sem = Arc::new(Semaphore::new(connections));
        let accept = TlsAcceptor::from(config);
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _=&mut rx => break,
                Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
                incoming=listener.accept()=>{
                    let (mut socket,_)=incoming?;
                    no_delay(&socket);
                    let Some(shared)=service.admission.connect() else {continue};
                    let Ok(permit)=sem.clone().try_acquire_owned() else {continue};
                    let accept=accept.clone();let service=service.clone();
                    tasks.spawn(async move {
                        let _permit=permit;
                        if crate::pgwire::ssl_request(&mut socket).await.is_err() {
                            eprintln!("protocol=postgres rejected stage=ssl_request reason=TLS_SSLRequest_required");return;
                        }
                        let Ok(Ok(mut stream))=timeout(IO_TIMEOUT,accept.accept(socket)).await else {
                            eprintln!("protocol=postgres rejected stage=tls reason=handshake_failed_or_timed_out");return;
                        };
                        let Some(cert)=stream.get_ref().1.peer_certificates().and_then(|c|c.first()) else {
                            eprintln!("protocol=postgres rejected stage=tls reason=client_certificate_required");return;
                        };
                        let Ok(mut session)=service.session(&tls::fingerprint(cert.as_ref())) else {
                            eprintln!("protocol=postgres rejected stage=binding reason=client_certificate_not_authorized");return;
                        };
                        session.connection=Some(shared);
                        session.backend_ssl=true;
                        if let Err(error)=crate::pgwire::run(&mut stream,session).await {
                            eprintln!("protocol=postgres closed stage=wire io_kind={:?}",error.kind());
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
