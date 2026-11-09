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
        builder
            .with_single_cert(chain, key)
            .map_err(io::Error::other)?,
    ))
}

/// Shared certificate resolver. Existing TLS streams retain their negotiated keys.
#[derive(Debug)]
pub struct ReloadableCertificate {
    resolver: std::sync::RwLock<Arc<dyn rustls::server::ResolvesServerCert>>,
}
impl ReloadableCertificate {
    pub fn install(config: &mut Arc<ServerConfig>) -> Arc<Self> {
        let resolver = Arc::new(Self {
            resolver: std::sync::RwLock::new(config.cert_resolver.clone()),
        });
        let config = Arc::make_mut(config);
        config.cert_resolver = resolver.clone();
        // Resumption skips certificate authentication, so require a fresh handshake.
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        config.send_tls13_tickets = 0;
        resolver
    }

    /// Validate all PEM and key correspondence before changing the live resolver.
    pub fn reload(
        &self,
        certificate: impl AsRef<Path>,
        private_key: impl AsRef<Path>,
    ) -> io::Result<()> {
        let candidate = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs(certificate)?, key(private_key)?)
            .map_err(io::Error::other)?;
        *self
            .resolver
            .write()
            .map_err(|_| io::Error::other("TLS resolver lock poisoned"))? = candidate.cert_resolver;
        Ok(())
    }
}
impl rustls::server::ResolvesServerCert for ReloadableCertificate {
    fn resolve(
        &self,
        hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        self.resolver.read().ok()?.resolve(hello)
    }
}
