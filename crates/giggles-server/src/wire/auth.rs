//! Transport selection and peer identity for role wire connections.

use crate::{
    api::{ConnectionIdentity, net::RoleConnection},
    auth::{RoleApiListener, TlsMode},
    tls,
    transport::IO_TIMEOUT,
};
use std::{io, net::SocketAddr};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::{Instant, timeout_at},
};
use tokio_rustls::TlsAcceptor;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Sniffed {
    Tls,
    Plain,
}

pub(crate) async fn sniff(stream: &TcpStream) -> io::Result<Sniffed> {
    let mut byte = [0];
    match timeout_at(Instant::now() + IO_TIMEOUT, stream.peek(&mut byte)).await?? {
        0 => Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
        _ => match byte[0] {
            0x16 => Ok(Sniffed::Tls),
            b'G' => Ok(Sniffed::Plain),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unrecognized wire transport",
            )),
        },
    }
}

pub(super) trait RoleWireIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> RoleWireIo for T {}

#[derive(Clone)]
pub(super) enum RoleWireAuth {
    Plain,
    Optional(TlsAcceptor),
    Required(TlsAcceptor),
}

impl RoleWireAuth {
    pub(super) fn new(settings: &RoleApiListener) -> io::Result<Self> {
        let tls = || {
            settings
                .wire_tls_config
                .clone()
                .map(TlsAcceptor::from)
                .ok_or_else(|| io::Error::other("role_api wire TLS configuration is missing"))
        };
        match settings.wire_mode {
            TlsMode::Disabled => Ok(Self::Plain),
            TlsMode::Optional => Ok(Self::Optional(tls()?)),
            TlsMode::Required => Ok(Self::Required(tls()?)),
        }
    }

    pub(super) async fn accept(
        self,
        socket: TcpStream,
        peer: SocketAddr,
    ) -> io::Result<(Box<dyn RoleWireIo>, ConnectionIdentity)> {
        // One deadline covers the sniff and the TLS handshake together, so a
        // client cannot spend IO_TIMEOUT on each stage.
        let deadline = Instant::now() + IO_TIMEOUT;
        let acceptor = match self {
            Self::Plain => None,
            Self::Optional(acceptor) => match timeout_at(deadline, sniff(&socket)).await?? {
                Sniffed::Plain => None,
                Sniffed::Tls => Some(acceptor),
            },
            Self::Required(acceptor) => Some(acceptor),
        };
        let tls = acceptor.is_some();
        let (stream, certificate): (Box<dyn RoleWireIo>, _) = if let Some(acceptor) = acceptor {
            let stream = timeout_at(deadline, acceptor.accept(socket)).await??;
            let certificate = stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|chain| chain.first())
                .map(|cert| tls::fingerprint(cert.as_ref()));
            (Box::new(stream), certificate)
        } else {
            (Box::new(socket), None)
        };
        let identity = ConnectionIdentity::role_connection(RoleConnection {
            peer,
            certificate,
            tls,
        });
        Ok((stream, identity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[tokio::test]
    async fn sniff_preserves_tls_and_plain_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        for (byte, expected) in [(0x16, Sniffed::Tls), (b'G', Sniffed::Plain)] {
            let mut client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            client.write_all(&[byte]).await.unwrap();
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(sniff(&socket).await.unwrap(), expected);
            assert_eq!(socket.read_u8().await.unwrap(), byte);
        }
    }

    #[tokio::test]
    async fn sniff_rejects_unrecognized_byte_and_eof() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        for (input, kind) in [
            (&[0_u8][..], io::ErrorKind::InvalidData),
            (&[][..], io::ErrorKind::UnexpectedEof),
        ] {
            let mut client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            client.write_all(input).await.unwrap();
            client.shutdown().await.unwrap();
            let (socket, _) = listener.accept().await.unwrap();
            assert_eq!(sniff(&socket).await.unwrap_err().kind(), kind);
        }
    }
}
