use crate::{Request, Response, Service, tls};
use rustls::{ClientConfig, ServerConfig, pki_types::ServerName};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, oneshot, watch},
    task::JoinHandle,
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector, client::TlsStream};
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
pub fn frame_len(n: u32, max: usize) -> io::Result<usize> {
    let n = usize::try_from(n).map_err(io::Error::other)?;
    if n == 0 || n > max {
        Err(io::Error::other("frame limit"))
    } else {
        Ok(n)
    }
}
pub async fn read_frame<S: AsyncRead + Unpin>(s: &mut S, max: usize) -> io::Result<Vec<u8>> {
    timeout(IO_TIMEOUT, async {
        let n = frame_len(s.read_u32().await?, max)?;
        let mut b = vec![0; n];
        s.read_exact(&mut b).await?;
        Ok(b)
    })
    .await
    .map_err(io::Error::other)?
}
pub async fn write_frame<S: AsyncWrite + Unpin>(s: &mut S, b: &[u8], max: usize) -> io::Result<()> {
    if b.is_empty() || b.len() > max {
        return Err(io::Error::other("frame limit"));
    }
    timeout(IO_TIMEOUT, async {
        s.write_u32(u32::try_from(b.len()).map_err(io::Error::other)?)
            .await?;
        s.write_all(b).await?;
        s.flush().await
    })
    .await
    .map_err(io::Error::other)?
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
pub async fn serve(
    address: SocketAddr,
    service: Arc<Service>,
    config: Arc<ServerConfig>,
    connections: usize,
) -> io::Result<Running> {
    serve_mode(address, service, config, connections, false).await
}
pub async fn serve_pg(
    address: SocketAddr,
    service: Arc<Service>,
    config: Arc<ServerConfig>,
    connections: usize,
) -> io::Result<Running> {
    serve_mode(address, service, config, connections, true).await
}
async fn serve_mode(
    address: SocketAddr,
    service: Arc<Service>,
    config: Arc<ServerConfig>,
    connections: usize,
    pg: bool,
) -> io::Result<Running> {
    if connections == 0 || connections > 128 {
        return Err(io::Error::other("connection limit"));
    }
    let listener = TcpListener::bind(address).await?;
    let (_, start) = watch::channel(true);
    serve_listener(listener, service, config, connections, pg, start).await
}

/// Starts a pre-bound native listener after the common startup gate opens.
pub async fn serve_bound(
    listener: TcpListener,
    service: Arc<Service>,
    config: Arc<ServerConfig>,
    connections: usize,
    start: watch::Receiver<bool>,
) -> io::Result<Running> {
    serve_listener(listener, service, config, connections, false, start).await
}

/// Starts a pre-bound PostgreSQL listener after the common startup gate opens.
pub async fn serve_pg_bound(
    listener: TcpListener,
    service: Arc<Service>,
    config: Arc<ServerConfig>,
    connections: usize,
    start: watch::Receiver<bool>,
) -> io::Result<Running> {
    serve_listener(listener, service, config, connections, true, start).await
}

async fn serve_listener(
    listener: TcpListener,
    service: Arc<Service>,
    config: Arc<ServerConfig>,
    connections: usize,
    pg: bool,
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
                        let protocol=if pg {"postgres"} else {"native"};
                        if pg && crate::pgwire::ssl_request(&mut socket).await.is_err() {
                            eprintln!("protocol={protocol} rejected stage=ssl_request reason=TLS_SSLRequest_required");return;
                        }
                        let Ok(Ok(mut stream))=timeout(IO_TIMEOUT,accept.accept(socket)).await else {
                            eprintln!("protocol={protocol} rejected stage=tls reason=handshake_failed_or_timed_out");return;
                        };
                        let Some(cert)=stream.get_ref().1.peer_certificates().and_then(|c|c.first()) else {
                            eprintln!("protocol={protocol} rejected stage=tls reason=client_certificate_required");return;
                        };
                        let Ok(mut session)=service.session(&tls::fingerprint(cert.as_ref())) else {
                            eprintln!("protocol={protocol} rejected stage=binding reason=client_certificate_not_authorized");return;
                        };
                        session.connection=Some(shared);
                        session.backend_ssl=true;
                        let blocking=service.blocking.clone();
                        if pg {
                            if let Err(error)=crate::pgwire::run(&mut stream,session).await {
                                eprintln!("protocol={protocol} closed stage=wire io_kind={:?}",error.kind());
                            }
                            return;
                        }
                        while let Ok(b)=read_frame(&mut stream,MAX_FRAME).await {
                            if !service.admission.request() { return; }
                            let received=std::time::Instant::now();
                            let Ok((returned,encoded))=blocking.run(move || {
                                let response=match serde_json::from_slice::<Request>(&b) {
                                    Ok(request)=>session.handle_received(request,received),
                                    Err(_)=>{session.abort();Response::code("08P01")},
                                };
                                (session,serde_json::to_vec(&response))
                            }).await else {return};
                            session=returned;
                            let Ok(b)=encoded else {break};
                            if session.check_binding().is_err() { return; }
                            if write_frame(&mut stream,&b,MAX_RESPONSE).await.is_err(){break;}
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
pub struct Client {
    stream: TlsStream<TcpStream>,
}
impl Client {
    pub async fn connect(
        address: SocketAddr,
        name: &str,
        config: Arc<ClientConfig>,
    ) -> io::Result<Self> {
        let name = ServerName::try_from(name.to_owned()).map_err(io::Error::other)?;
        let stream = timeout(IO_TIMEOUT, async {
            let socket = TcpStream::connect(address).await?;
            no_delay(&socket);
            TlsConnector::from(config).connect(name, socket).await
        })
        .await
        .map_err(io::Error::other)??;
        Ok(Self { stream })
    }
    /// Never retries. A missing response to writes is ambiguous.
    pub async fn call(&mut self, r: &Request) -> io::Result<Response> {
        write_frame(&mut self.stream, &serde_json::to_vec(r)?, MAX_FRAME).await?;
        serde_json::from_slice(&read_frame(&mut self.stream, MAX_RESPONSE).await?)
            .map_err(io::Error::other)
    }
}
