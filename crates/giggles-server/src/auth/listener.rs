//! Role-authenticated PostgreSQL listener, isolated from agent certificate bindings.
use super::{
    scram::{MECHANISM, ServerExchange, Verifier},
    signin::{self, Method, SigninPolicy},
};
use crate::{
    Service,
    pgwire_startup::{StartupPolicy, parse_startup_with_diagnostics, read_startup_packet, reject_invalid_startup, reject_startup},
    transport::{IO_TIMEOUT, Running},
};
use std::{collections::BTreeMap, io, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch},
    time::timeout,
};
use tokio_rustls::TlsAcceptor;

pub struct PostgresListener {
    pub tls: super::config::TlsMode,
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
    pub certificates: BTreeMap<String, String>,
    pub databases: Vec<String>,
    pub loopback_trust: bool,
    pub connections: usize,
}

pub async fn serve_postgresql(
    address: SocketAddr,
    service: Arc<Service>,
    listener: Arc<PostgresListener>,
) -> io::Result<Running> {
    let socket = TcpListener::bind(address).await?;
    let (_, start) = watch::channel(true);
    serve_postgresql_bound(socket, service, listener, start).await
}

pub async fn serve_postgresql_bound(
    socket: TcpListener,
    service: Arc<Service>,
    listener: Arc<PostgresListener>,
    mut start: watch::Receiver<bool>,
) -> io::Result<Running> {
    if listener.connections == 0 || listener.connections > 128 {
        return Err(io::Error::other("connection limit"));
    }
    let address = socket.local_addr()?;
    let (stop, mut rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        while !*start.borrow_and_update() {
            tokio::select! {
                _ = &mut rx => return Ok(()),
                changed = start.changed() => if changed.is_err() { return Ok(()); },
            }
        }
        let sem = Arc::new(Semaphore::new(listener.connections));
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut rx => break,
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                incoming = socket.accept() => {
                    let (socket, peer) = incoming?;
                    crate::transport::no_delay(&socket);
                    let Some(shared) = service.admission.connect() else { continue; };
                    let Ok(permit) = sem.clone().try_acquire_owned() else { continue; };
                    let service = service.clone();
                    let listener = listener.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        if let Err(error) = connection(socket, peer, service, listener, shared).await {
                            eprintln!("protocol=postgresql rejected stage=wire reason={:?}", error.kind());
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

trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

async fn connection(
    socket: TcpStream,
    peer: SocketAddr,
    service: Arc<Service>,
    listener: Arc<PostgresListener>,
    shared: Arc<OwnedSemaphorePermit>,
) -> io::Result<()> {
    if !service.admission.request() {
        return Ok(());
    }
    let mut stream: Box<dyn Stream> = Box::new(socket);
    let mut encrypted = false;
    let mut certificate = None;
    let mut ssl_seen = false;
    let mut negotiations = 0;
    let body = loop {
        let body = read_startup_packet(&mut stream).await?;
        let code = u32::from_be_bytes(body[..4].try_into().unwrap());
        match code {
            80877103 | 80877104 => {
                negotiations += 1;
                if body.len() != 4
                    || negotiations > 2
                    || (code == 80877103 && (ssl_seen || encrypted))
                {
                    return fatal(&mut stream, "08P01", "invalid negotiation packet").await;
                }
                if code == 80877103 {
                    ssl_seen = true;
                    if let Some(config) = &listener.tls_config {
                        send(&mut stream, b"S").await?;
                        let tls =
                            timeout(IO_TIMEOUT, TlsAcceptor::from(config.clone()).accept(stream))
                                .await
                                .map_err(io::Error::other)??;
                        certificate = tls
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|certs| certs.first())
                            .map(|cert| crate::tls::fingerprint(cert.as_ref()));
                        stream = Box::new(tls);
                        encrypted = true;
                        continue;
                    }
                }
                send(&mut stream, b"N").await?;
            }
            80877102 => {
                if body.len() != 12 {
                    return fatal(&mut stream, "08P01", "invalid cancellation packet").await;
                }
                service.cancellations.cancel_key(
                    "pgrole:",
                    u32::from_be_bytes(body[4..8].try_into().unwrap()),
                    u32::from_be_bytes(body[8..12].try_into().unwrap()),
                );
                return Ok(());
            }
            _ => break body,
        }
    };
    if !encrypted && matches!(listener.tls, super::config::TlsMode::Required) {
        eprintln!("protocol=postgresql rejected stage=startup reason=TLS_required");
        return reject_startup(&mut stream, "28000", "TLS is required").await;
    }
    let startup = match parse_startup_with_diagnostics(
        &body,
        &StartupPolicy::Postgresql {
            databases: &listener.databases,
        },
    ) {
        Ok(startup) => startup,
        Err(error) => {
            eprintln!("protocol=postgresql rejected stage=startup reason=invalid_startup");
            return reject_invalid_startup(&mut stream, error).await;
        }
    };
    if let Method::Password(verifier) = signin::choose(
        SigninPolicy {
            certificates: &listener.certificates,
            loopback_trust: listener.loopback_trust,
        },
        peer,
        certificate.as_deref(),
        &startup.user,
        &service,
    ) && !authenticate(&mut stream, &startup.user, verifier).await?
    {
        return Ok(());
    }
    let mut session = match signin::open_role_session(&service, &startup.user) {
        Ok(session) => session,
        Err(_) => return fatal(&mut stream, "28P01", "password authentication failed").await,
    };
    session.connection = Some(shared);
    session.backend_ssl = encrypted;
    session.database = Some(startup.database.clone());
    if !service.admission.request() {
        return Ok(());
    }
    let superuser = service
        .admin
        .roles()
        .get(&startup.user)
        .ok()
        .flatten()
        .is_some_and(|role| role.superuser);
    let statuses = [
        (
            "is_superuser",
            if superuser { "on" } else { "off" }.to_owned(),
        ),
        ("session_authorization", startup.user.clone()),
    ];
    crate::pgwire::serve_session(&mut stream, session, startup, false, &statuses).await
}

async fn send<S: AsyncWrite + Unpin>(stream: &mut S, bytes: &[u8]) -> io::Result<()> {
    timeout(IO_TIMEOUT, async {
        stream.write_all(bytes).await?;
        stream.flush().await
    })
    .await
    .map_err(io::Error::other)?
}

async fn authentication<S: AsyncWrite + Unpin>(
    stream: &mut S,
    kind: u32,
    payload: &[u8],
) -> io::Result<()> {
    let mut frame = vec![b'R'];
    frame.extend_from_slice(&((8 + payload.len()) as u32).to_be_bytes());
    frame.extend_from_slice(&kind.to_be_bytes());
    frame.extend_from_slice(payload);
    send(stream, &frame).await
}

async fn fatal<S: AsyncWrite + Unpin>(stream: &mut S, code: &str, message: &str) -> io::Result<()> {
    eprintln!("protocol=postgresql rejected stage=authentication reason={code}");
    let mut body = b"SFATAL\0VFATAL\0C".to_vec();
    crate::pgwire::z(code, &mut body);
    body.push(b'M');
    crate::pgwire::z(message, &mut body);
    body.push(0);
    let mut frame = vec![b'E'];
    frame.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    send(stream, &frame).await
}

async fn password_message<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Option<Vec<u8>>> {
    timeout(IO_TIMEOUT, async {
        let kind = stream.read_u8().await?;
        let length = stream.read_u32().await?;
        if kind != b'p' || !(4..=2052).contains(&length) {
            return Ok(None);
        }
        let mut body = vec![0; length as usize - 4];
        stream.read_exact(&mut body).await?;
        Ok(Some(body))
    })
    .await
    .map_err(io::Error::other)?
}

fn initial_response(body: &[u8]) -> Option<(&str, &[u8])> {
    let end = body.iter().position(|byte| *byte == 0)?;
    let mechanism = std::str::from_utf8(&body[..end]).ok()?;
    let rest = body.get(end + 1..)?;
    let length = i32::from_be_bytes(rest.get(..4)?.try_into().ok()?);
    let data = rest.get(4..)?;
    if usize::try_from(length).ok()? != data.len() {
        return None;
    }
    Some((mechanism, data))
}

async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    user: &str,
    verifier: Option<Verifier>,
) -> io::Result<bool> {
    let mut mechanisms = MECHANISM.as_bytes().to_vec();
    mechanisms.extend_from_slice(&[0, 0]);
    authentication(stream, 10, &mechanisms).await?;
    let mut exchange = ServerExchange::new(user, verifier);
    let Some(body) = password_message(stream).await? else {
        fatal(stream, "08P01", "invalid SCRAM exchange").await?;
        return Ok(false);
    };
    let Some((mechanism, data)) = initial_response(&body) else {
        fatal(stream, "08P01", "invalid SCRAM exchange").await?;
        return Ok(false);
    };
    let Ok(first) = exchange.client_first(mechanism, data) else {
        fatal(stream, "28P01", "password authentication failed").await?;
        return Ok(false);
    };
    authentication(stream, 11, &first).await?;
    let Some(body) = password_message(stream).await? else {
        fatal(stream, "08P01", "invalid SCRAM exchange").await?;
        return Ok(false);
    };
    let Ok(final_message) = exchange.client_final(&body) else {
        fatal(stream, "28P01", "password authentication failed").await?;
        return Ok(false);
    };
    authentication(stream, 12, &final_message).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_response_requires_exact_nonnegative_length() {
        let mut body = b"SCRAM-SHA-256\0".to_vec();
        body.extend_from_slice(&3i32.to_be_bytes());
        body.extend_from_slice(b"abc");
        assert_eq!(
            initial_response(&body),
            Some((MECHANISM, b"abc".as_slice()))
        );
        body.push(0);
        assert!(initial_response(&body).is_none());
        assert!(initial_response(b"SCRAM-SHA-256\0\xff\xff\xff\xff").is_none());
        assert!(initial_response(b"SCRAM-SHA-256\0\0\0\0").is_none());
        assert!(initial_response(b"SCRAM-SHA-256").is_none());
    }

    #[tokio::test]
    async fn password_frame_boundaries_and_fatal_severity() {
        for (kind, length) in [(b'Q', 4u32), (b'p', 3), (b'p', 2053)] {
            let mut frame = vec![kind];
            frame.extend_from_slice(&length.to_be_bytes());
            assert!(
                password_message(&mut frame.as_slice())
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let mut frame = vec![b'p'];
        frame.extend_from_slice(&2052u32.to_be_bytes());
        frame.extend_from_slice(&[b'x'; 2048]);
        assert_eq!(
            password_message(&mut frame.as_slice())
                .await
                .unwrap()
                .unwrap()
                .len(),
            2048
        );
        let mut output = Vec::new();
        fatal(&mut output, "28P01", "password authentication failed")
            .await
            .unwrap();
        assert!(output[5..].starts_with(b"SFATAL\0VFATAL\0C"));
    }
}
