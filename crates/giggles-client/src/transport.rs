use crate::{ClientError, Config, Operation};
use giggles_consensus::native::{self, Response};
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use std::{
    future::Future,
    io::{BufReader, Read},
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, timeout_at},
};
use tokio_rustls::TlsConnector;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Failure {
    BeforeSend,
    Tls,
    Timeout,
    AfterSend,
    Protocol,
}
pub(crate) type Exchange<'a> = Pin<Box<dyn Future<Output = Result<Response, Failure>> + Send + 'a>>;
pub(crate) trait Transport: Send + Sync {
    fn exchange<'a>(
        &'a self,
        endpoint: SocketAddr,
        op: &'a Operation,
        deadline: Instant,
        connect: Duration,
        sent: &'a AtomicBool,
    ) -> Exchange<'a>;
}
pub(crate) struct Tls {
    connector: TlsConnector,
    name: ServerName<'static>,
}
// Credential files are bounded independently of application frames.
fn pem(path: &std::path::Path) -> Result<BufReader<std::io::Cursor<Vec<u8>>>, ClientError> {
    let mut data = Vec::new();
    std::fs::File::open(path)
        .and_then(|f| f.take(1024 * 1024 + 1).read_to_end(&mut data))
        .map_err(|_| ClientError::Credentials)?;
    if data.len() > 1024 * 1024 {
        return Err(ClientError::Credentials);
    }
    Ok(BufReader::new(std::io::Cursor::new(data)))
}
impl Tls {
    pub(crate) fn new(config: &Config) -> Result<Self, ClientError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut pem(&config.ca)?) {
            roots
                .add(cert.map_err(|_| ClientError::Credentials)?)
                .map_err(|_| ClientError::Credentials)?;
        }
        if roots.is_empty() {
            return Err(ClientError::Credentials);
        }
        let chain = rustls_pemfile::certs(&mut pem(&config.certificate)?)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ClientError::Credentials)?;
        let key = rustls_pemfile::private_key(&mut pem(&config.key)?)
            .map_err(|_| ClientError::Credentials)?
            .ok_or(ClientError::Credentials)?;
        let mut tls = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| ClientError::Credentials)?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .map_err(|_| ClientError::Credentials)?;
        tls.alpn_protocols = vec![b"giggles-native/1".to_vec()];
        Ok(Self {
            connector: TlsConnector::from(Arc::new(tls)),
            name: ServerName::try_from(config.server_name.clone())
                .map_err(|_| ClientError::InvalidConfig)?,
        })
    }
}
impl Transport for Tls {
    fn exchange<'a>(
        &'a self,
        endpoint: SocketAddr,
        op: &'a Operation,
        deadline: Instant,
        connect: Duration,
        sent: &'a AtomicBool,
    ) -> Exchange<'a> {
        Box::pin(async move {
            let connected = timeout_at(deadline.min(Instant::now() + connect), async {
                let tcp = TcpStream::connect(endpoint)
                    .await
                    .map_err(|_| Failure::BeforeSend)?;
                self.connector
                    .connect(self.name.clone(), tcp)
                    .await
                    .map_err(|_| Failure::Tls)
            })
            .await
            .map_err(|_| Failure::Timeout)??;
            let mut tls = connected;
            // Strict ALPN prevents accidentally talking to a different service.
            if tls.get_ref().1.alpn_protocol() != Some(b"giggles-native/1") {
                return Err(Failure::Tls);
            }
            timeout_at(deadline, async {
                // Flip before the first write: even a partial prefix may have arrived.
                sent.store(true, Ordering::Release);
                tls.write_u32(op.bytes.len() as u32)
                    .await
                    .map_err(|_| Failure::AfterSend)?;
                tls.write_all(&op.bytes)
                    .await
                    .map_err(|_| Failure::AfterSend)?;
                tls.flush().await.map_err(|_| Failure::AfterSend)?;
                let size = tls.read_u32().await.map_err(|_| Failure::AfterSend)? as usize;
                if size == 0 || size > native::MAX_FRAME {
                    return Err(Failure::Protocol);
                }
                let mut bytes = vec![0; size];
                tls.read_exact(&mut bytes)
                    .await
                    .map_err(|_| Failure::AfterSend)?;
                let response: Response =
                    serde_json::from_slice(&bytes).map_err(|_| Failure::Protocol)?;
                if response.version != 1
                    || response.id != op.id
                    || response.reply.is_some() == response.error.is_some()
                    || response.reply.as_ref().is_some_and(|r| r.version != 1)
                {
                    return Err(Failure::Protocol);
                }
                Ok(response)
            })
            .await
            .map_err(|_| Failure::Timeout)?
        })
    }
}
