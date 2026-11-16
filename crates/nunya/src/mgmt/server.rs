//! Bounded HTTP management listeners and graceful shutdown.
use super::{
    auth::Origin,
    routes::{self, MgmtContext},
};
use crate::config::{self, ManagementSection};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{
    convert::Infallible,
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::{Semaphore, watch},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_rustls::TlsAcceptor;

const IO_TIMEOUT: Duration = Duration::from_secs(10);
const BODY_LIMIT: usize = 64 * 1024;

/// Sanitized listener startup refusal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpawnError {
    /// Non-loopback listeners require TLS.
    TlsRequired,
    /// TLS configuration could not be loaded.
    Tls,
    /// TCP listener could not be bound.
    Bind,
    /// Local socket could not be installed securely.
    Socket,
}
impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TlsRequired => "management TLS required",
            Self::Tls => "management TLS unavailable",
            Self::Bind => "management listener unavailable",
            Self::Socket => "management socket unavailable",
        })
    }
}
impl std::error::Error for SpawnError {}

/// Bound endpoints and completion of the management server.
pub struct ServerHandle {
    /// Actual bound TCP address, including an allocated ephemeral port.
    pub tcp_addr: SocketAddr,
    /// Local socket path on Unix.
    pub socket_path: Option<PathBuf>,
    task: JoinHandle<()>,
}
impl ServerHandle {
    /// Wait until listeners and in-flight connections have stopped.
    pub async fn wait(self) {
        let _ = self.task.await;
    }
}
async fn stopped(rx: &mut watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() || rx.changed().await.is_err() {
            return;
        }
    }
}
fn failure(status: StatusCode, code: &'static str) -> Response<Full<Bytes>> {
    let mut response = routes::error(status, code).map(Full::new);
    response.headers_mut().insert(
        http::header::CONNECTION,
        http::HeaderValue::from_static("close"),
    );
    response
}
async fn request(
    ctx: Arc<MgmtContext>,
    origin: Origin,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if req
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .is_some_and(|n| n > BODY_LIMIT as u64)
    {
        return Ok(failure(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"));
    }
    let (parts, body) = req.into_parts();
    let body = match timeout(IO_TIMEOUT, Limited::new(body, BODY_LIMIT).collect()).await {
        Ok(Ok(body)) => body.to_bytes(),
        Ok(Err(err)) if err.is::<http_body_util::LengthLimitError>() => {
            return Ok(failure(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"));
        }
        Ok(Err(_)) | Err(_) => return Ok(failure(StatusCode::BAD_REQUEST, "bad_request")),
    };
    let response = tokio::task::spawn_blocking(move || {
        routes::handle(&ctx, origin, Request::from_parts(parts, body))
    })
    .await;
    Ok(match response {
        Ok(response) => response.map(Full::new),
        Err(_) => failure(StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
    })
}
async fn connection<I>(
    io: I,
    ctx: Arc<MgmtContext>,
    origin: Origin,
    mut stop: watch::Receiver<bool>,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |req| request(ctx.clone(), origin, req));
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(IO_TIMEOUT)
        .max_buf_size(16 * 1024);
    let conn = builder.serve_connection(TokioIo::new(io), service);
    tokio::pin!(conn);
    tokio::select! {
        _ = &mut conn => {},
        _ = stopped(&mut stop) => {
            conn.as_mut().graceful_shutdown();
            let _ = conn.await;
        }
    }
}

#[cfg(unix)]
fn socket(state_dir: &Path) -> Result<(tokio::net::UnixListener, PathBuf), SpawnError> {
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    let path = state_dir.join("nunya.sock");
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if !meta.file_type().is_socket() => return Err(SpawnError::Socket),
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(SpawnError::Socket),
    }
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).map_err(|_| SpawnError::Socket)?;
    let temp = state_dir.join(format!(
        ".nunya.sock.{}",
        data_encoding::HEXLOWER.encode(&nonce)
    ));
    let listener = tokio::net::UnixListener::bind(&temp).map_err(|_| SpawnError::Socket)?;
    let install = || {
        std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o600))?;
        // Recheck before replacement; never deliberately replace a regular file.
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if !meta.file_type().is_socket() => {
                return Err(std::io::Error::other("socket unavailable"));
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        std::fs::rename(&temp, &path)
    };
    if install().is_err() {
        let _ = std::fs::remove_file(&temp);
        return Err(SpawnError::Socket);
    }
    Ok((listener, path))
}

/// Start bounded TCP and local management listeners without wiring the main service.
pub async fn spawn(
    ctx: Arc<MgmtContext>,
    section: &ManagementSection,
    state_dir: &Path,
    mut shutdown: watch::Receiver<bool>,
) -> Result<ServerHandle, SpawnError> {
    if !section.listen.ip().is_loopback() && section.tls.is_none() {
        return Err(SpawnError::TlsRequired);
    }
    let tls = section
        .tls
        .as_ref()
        .map(config::load_tls)
        .transpose()
        .map_err(|_| SpawnError::Tls)?
        .map(TlsAcceptor::from);
    let tcp = TcpListener::bind(section.listen)
        .await
        .map_err(|_| SpawnError::Bind)?;
    let tcp_addr = tcp.local_addr().map_err(|_| SpawnError::Bind)?;
    #[cfg(unix)]
    let (local, path) = socket(state_dir)?;
    #[cfg(unix)]
    let socket_path = Some(path.clone());
    #[cfg(not(unix))]
    let socket_path = {
        let _ = state_dir;
        None
    };
    let task = tokio::spawn(async move {
        let permits = Arc::new(Semaphore::new(64));
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = stopped(&mut shutdown) => break,
                Some(_) = connections.join_next(), if !connections.is_empty() => {},
                accepted = tcp.accept() => {
                    let Ok((io, _)) = accepted else {
                        // Retry transient accept failures without delaying shutdown.
                        tokio::select! {
                            _ = stopped(&mut shutdown) => break,
                            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                        }
                        continue;
                    };
                    let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                    let ctx = ctx.clone();
                    let tls = tls.clone();
                    let mut stop = stop_rx.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        if let Some(tls) = tls {
                            let io = tokio::select! {
                                _ = stopped(&mut stop) => return,
                                result = timeout(IO_TIMEOUT, tls.accept(io)) => match result {
                                    Ok(Ok(io)) => io,
                                    Ok(Err(_)) | Err(_) => return,
                                }
                            };
                            connection(io, ctx, Origin::Network, stop).await;
                        } else {
                            connection(io, ctx, Origin::Network, stop).await;
                        }
                    });
                }
                accepted = async {
                    #[cfg(unix)]
                    { local.accept().await }
                    #[cfg(not(unix))]
                    { std::future::pending::<std::io::Result<(tokio::net::TcpStream, SocketAddr)>>().await }
                } => {
                    let Ok((io, _)) = accepted else {
                        // Retry transient accept failures without delaying shutdown.
                        tokio::select! {
                            _ = stopped(&mut shutdown) => break,
                            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                        }
                        continue;
                    };
                    let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                    let ctx = ctx.clone();
                    let stop = stop_rx.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        connection(io, ctx, Origin::Socket, stop).await;
                    });
                }
            }
        }
        drop(tcp);
        #[cfg(unix)]
        drop(local);
        let _ = stop_tx.send(true);
        if timeout(Duration::from_secs(5), async {
            while connections.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
        #[cfg(unix)]
        let _ = std::fs::remove_file(path);
    });
    Ok(ServerHandle {
        tcp_addr,
        socket_path,
        task,
    })
}
