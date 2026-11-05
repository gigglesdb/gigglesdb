use crate::{Request, Response, Service, tls};
use rustls::{ClientConfig, ServerConfig, pki_types::ServerName};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector, client::TlsStream};
pub const MAX_FRAME: usize = 256 * 1024;
pub const MAX_RESPONSE: usize = 8 * 1024 * 1024;
pub const IO_TIMEOUT: Duration = Duration::from_secs(10);
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
    pub async fn shutdown(mut self) -> io::Result<()> {
        if let Some(s) = self.stop.take() {
            let _ = s.send(());
        }
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
    let address = listener.local_addr()?;
    let (stop, mut rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(connections));
        let accept = TlsAcceptor::from(config);
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _=&mut rx => break,
                Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
                incoming=listener.accept()=>{
                    let (mut socket,_)=incoming?;
                    let Some(shared)=service.admission.connect() else {continue};
                    let Ok(permit)=sem.clone().try_acquire_owned() else {continue};
                    let accept=accept.clone();let service=service.clone();
                    tasks.spawn(async move {
                        let _permit=permit;
                        if pg && crate::pgwire::ssl_request(&mut socket).await.is_err() {return;}
                        let Ok(Ok(mut stream))=timeout(IO_TIMEOUT,accept.accept(socket)).await else {return};
                        let Some(cert)=stream.get_ref().1.peer_certificates().and_then(|c|c.first()) else {return};
                        let Ok(mut session)=service.session(&tls::fingerprint(cert.as_ref())) else {return};
                        session.connection=Some(shared);
                        let blocking=service.blocking.clone();
                        if pg {let _=crate::pgwire::run(&mut stream,session).await;return;}
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
    Ok(Running {
        address,
        stop: Some(stop),
        task,
    })
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
