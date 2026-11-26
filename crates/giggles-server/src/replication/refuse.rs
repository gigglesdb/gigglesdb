//! PostgreSQL startup refusal for replication followers.
use std::{io, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, oneshot},
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::TlsAcceptor;

/// Serve PostgreSQL's retryable follower error on an already bound listener.
pub async fn serve_refusal_bound(
    listener: TcpListener,
    tls: Option<Arc<rustls::ServerConfig>>,
    hint: Arc<dyn Fn() -> String + Send + Sync>,
    connections: usize,
) -> io::Result<crate::Running> {
    if !(1..=128).contains(&connections) {
        return Err(io::Error::other("connection limit"));
    }
    let address = listener.local_addr()?;
    let (stop, mut rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(connections));
        let rejections = crate::auth::listener::ConnectionRejections::new();
        let accept = tls.clone().map(TlsAcceptor::from);
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = &mut rx => break,
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                incoming = listener.accept() => {
                    let (socket, _) = incoming?;
                    let Ok(permit) = sem.clone().try_acquire_owned() else {
                        rejections.reject(&mut tasks, socket, accept.as_ref(), "postgresql");
                        continue;
                    };
                    let tls = tls.clone();
                    let hint = hint.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        let _ = timeout(crate::IO_TIMEOUT, connection(socket, tls, hint)).await;
                    });
                }
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    });
    Ok(crate::Running::new(address, stop, task))
}

trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

async fn connection(
    socket: TcpStream,
    tls: Option<Arc<rustls::ServerConfig>>,
    hint: Arc<dyn Fn() -> String + Send + Sync>,
) -> io::Result<()> {
    let mut stream: Box<dyn Stream> = Box::new(socket);
    let mut ssl_seen = false;
    let mut negotiations = 0;
    loop {
        let body = crate::pgwire_startup::read_startup_packet(&mut stream).await?;
        let code = u32::from_be_bytes(body[..4].try_into().unwrap());
        match code {
            80877103 | 80877104 => {
                negotiations += 1;
                if body.len() != 4 || negotiations > 2 || (code == 80877103 && ssl_seen) {
                    return Ok(());
                }
                if code == 80877103 {
                    ssl_seen = true;
                    if let Some(config) = &tls {
                        stream.write_all(b"S").await?;
                        stream.flush().await?;
                        stream = Box::new(TlsAcceptor::from(config.clone()).accept(stream).await?);
                        continue;
                    }
                }
                stream.write_all(b"N").await?;
                stream.flush().await?;
            }
            196608 => {
                let mut body = Vec::new();
                for (field, value) in [
                    (b'S', "FATAL"),
                    (b'V', "FATAL"),
                    (b'C', "57P03"),
                    (b'M', "the database system is not accepting connections"),
                    (b'D', hint().as_str()),
                ] {
                    body.push(field);
                    crate::pgwire::z(value, &mut body);
                }
                body.push(0);
                stream.write_u8(b'E').await?;
                stream.write_u32((body.len() + 4) as u32).await?;
                stream.write_all(&body).await?;
                return stream.flush().await;
            }
            // CancelRequest and unsupported protocol versions silently close.
            _ => return Ok(()),
        }
    }
}
