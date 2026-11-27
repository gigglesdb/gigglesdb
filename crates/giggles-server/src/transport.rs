use crate::{Service, auth::listener::ConnectionRejections, tls};
use giggles_core::DurableTracker;
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
    pgwire_work: Option<(Arc<Service>, Arc<DurableTracker>)>,
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
            pgwire_work: None,
        }
    }

    pub(crate) fn with_pgwire_work(
        mut self,
        service: Arc<Service>,
        work: Arc<DurableTracker>,
    ) -> Self {
        self.pgwire_work = Some((service, work));
        self
    }

    /// Signal every listener before awaiting any shutdown work.
    pub fn request_shutdown(&mut self) {
        if let Some(s) = self.stop.take() {
            let _ = s.send(());
        }
    }

    /// Stops the listener and waits for its PostgreSQL jobs to release the service.
    pub async fn shutdown(mut self) -> io::Result<()> {
        self.stop_listener().await?;
        if let Some((service, work)) = self.pgwire_work {
            service.wait_for_pgwire_work(&work).await?;
        }
        Ok(())
    }

    /// Stops the listener and its socket tasks. Blocking jobs already started
    /// may still hold the service. Use only when the data directory stays open
    /// in this process.
    pub async fn shutdown_without_release(mut self) -> io::Result<()> {
        self.stop_listener().await
    }

    async fn stop_listener(&mut self) -> io::Result<()> {
        self.request_shutdown();
        (&mut self.task).await.map_err(io::Error::other)?
    }
}
/// Accepted sessions acquire admission before TLS allocation. A separate budget
/// covers TLS for overload replies. Shutdown aborts socket tasks and waits for
/// their outstanding blocking work.
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
    let work = Arc::new(DurableTracker::default());
    let task_work = work.clone();
    let task_service = service.clone();
    let task = tokio::spawn(async move {
        while !*start.borrow_and_update() {
            tokio::select! {
                _ = &mut rx => return Ok(()),
                changed = start.changed() => if changed.is_err() { return Ok(()); },
            }
        }
        let sem = Arc::new(Semaphore::new(connections));
        let rejections = ConnectionRejections::new().with_cancel(
            task_service.clone(), crate::auth::listener::CancelListener::Bound,
        );
        let accept = TlsAcceptor::from(config);
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _=&mut rx => break,
                Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
                incoming=listener.accept()=>{
                    let (mut socket,_)=incoming?;
                    no_delay(&socket);
                    let (Some(shared), Ok(permit)) =
                        (task_service.admission.connect(), sem.clone().try_acquire_owned())
                    else {
                        rejections.reject(&mut tasks, socket, Some(&accept), "postgres");
                        continue;
                    };
                    let accept=accept.clone();let service=task_service.clone();
                    let work = task_work.clone();
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
                        session.max_connections=Some(connections);
                        session.pgwire_listener_work = Some(work);
                        if let Err(error)=crate::pgwire::run(&mut stream,session).await {
                            eprintln!("protocol=postgres closed stage=wire io_kind={:?}",error.kind());
                        }
                    });
                }
            }
        }
        drop(listener);
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    });
    Ok(Running::new(address, stop, task).with_pgwire_work(service, work))
}
