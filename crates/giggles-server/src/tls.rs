use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use sha2::{Digest, Sha256};
use std::{
    io::{self, BufReader},
    path::Path,
    sync::Arc,
};
pub fn fingerprint(cert: &[u8]) -> String {
    hex::encode(Sha256::digest(cert))
}
pub fn certs(path: impl AsRef<Path>) -> io::Result<Vec<CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(path)?)).collect()
}
pub fn key(path: impl AsRef<Path>) -> io::Result<PrivateKeyDer<'static>> {
    rustls_pemfile::private_key(&mut BufReader::new(std::fs::File::open(path)?))?
        .ok_or_else(|| io::Error::other("missing private key"))
}
pub fn roots(certs: Vec<CertificateDer<'static>>) -> io::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(io::Error::other)?;
    }
    Ok(roots)
}
pub fn server(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    ca: Vec<CertificateDer<'static>>,
) -> io::Result<Arc<ServerConfig>> {
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots(ca)?))
        .build()
        .map_err(io::Error::other)?;
    Ok(Arc::new(
        ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key)
            .map_err(io::Error::other)?,
    ))
}
pub fn client(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    ca: Vec<CertificateDer<'static>>,
) -> io::Result<Arc<ClientConfig>> {
    Ok(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots(ca)?)
            .with_client_auth_cert(chain, key)
            .map_err(io::Error::other)?,
    ))
}

pub fn server_optional_client(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    ca: Option<Vec<CertificateDer<'static>>>,
) -> io::Result<Arc<ServerConfig>> {
    let builder = ServerConfig::builder();
    let builder = match ca {
        Some(ca) => builder.with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder(Arc::new(roots(ca)?))
                .allow_unauthenticated()
                .build()
                .map_err(io::Error::other)?,
        ),
        None => builder.with_no_client_auth(),
    };
    Ok(Arc::new(
        builder.with_single_cert(chain, key).map_err(io::Error::other)?,
    ))
}
