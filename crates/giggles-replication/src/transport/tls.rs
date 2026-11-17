use super::config::{TransportConfig, fingerprint};
use crate::error::{Error, Result};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};
use std::{io::BufReader, path::Path, sync::Arc};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub(super) fn chain(path: &Path, field: &str) -> Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path).map_err(|e| Error::Io(format!("{field}: {e}")))?;
    rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<std::io::Result<_>>()
        .map_err(|e| Error::Identity(format!("{field}: {e}")))
}

fn key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path).map_err(|e| Error::Io(format!("key: {e}")))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|e| Error::Identity(format!("key: {e}")))?
        .ok_or_else(|| Error::Identity("key: missing private key".into()))
}

pub fn tls(config: &TransportConfig) -> Result<(TlsAcceptor, TlsConnector)> {
    config.validate()?;
    let mut roots = RootCertStore::empty();
    for cert in chain(&config.ca, "ca")? {
        roots
            .add(cert)
            .map_err(|e| Error::Identity(format!("ca: {e}")))?;
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots.clone()))
        .build()
        .map_err(|e| Error::Identity(format!("ca: {e}")))?;
    let server = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            chain(&config.certificate, "certificate")?,
            key(&config.key)?,
        )
        .map_err(|e| Error::Identity(format!("certificate/key: {e}")))?;
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            chain(&config.certificate, "certificate")?,
            key(&config.key)?,
        )
        .map_err(|e| Error::Identity(format!("certificate/key: {e}")))?;
    Ok((
        TlsAcceptor::from(Arc::new(server)),
        TlsConnector::from(Arc::new(client)),
    ))
}

pub fn peer_id(config: &TransportConfig, leaf_der: &[u8]) -> Option<u64> {
    let fp = fingerprint(leaf_der);
    config
        .peers
        .iter()
        .find_map(|(&id, p)| (id != config.node_id && p.fingerprint == fp).then_some(id))
}

pub async fn connect(
    config: &TransportConfig,
    connector: &TlsConnector,
    id: u64,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    if id == config.node_id {
        return Err(Error::Identity(
            "cannot connect Raft transport to own node".into(),
        ));
    }
    connect_admin(config, connector, id).await
}

pub(crate) async fn connect_admin(
    config: &TransportConfig,
    connector: &TlsConnector,
    id: u64,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let peer = config
        .peers
        .get(&id)
        .ok_or_else(|| Error::Identity(format!("peers[{id}]: unknown or own node")))?;
    let tcp = TcpStream::connect(&peer.address).await?;
    tcp.set_nodelay(true)?;
    let stream = connector
        .connect(
            ServerName::try_from("localhost").expect("static DNS name"),
            tcp,
        )
        .await?;
    let leaf = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|c| c.first())
        .ok_or_else(|| Error::Identity(format!("peers[{id}].fingerprint: missing certificate")))?;
    if fingerprint(leaf) != peer.fingerprint {
        return Err(Error::Identity(format!(
            "peers[{id}].fingerprint: server mismatch"
        )));
    }
    Ok(stream)
}

pub async fn accept(
    config: &TransportConfig,
    acceptor: &TlsAcceptor,
    tcp: TcpStream,
) -> Result<(u64, tokio_rustls::server::TlsStream<TcpStream>)> {
    tcp.set_nodelay(true)?;
    let stream = acceptor.accept(tcp).await?;
    let id = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|c| c.first())
        .and_then(|leaf| peer_id(config, leaf))
        .ok_or_else(|| {
            Error::Identity("peers.fingerprint: unknown or own client certificate".into())
        })?;
    Ok((id, stream))
}
