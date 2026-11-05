use crate::HarnessError;
use giggles_client::{Config, Limits};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, KeyUsagePurpose};
use rustls::{RootCertStore, ServerConfig, pki_types::PrivatePkcs8KeyDer};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

pub(crate) fn fresh() -> Result<PathBuf, HarnessError> {
    for _ in 0..100 {
        let path = std::env::temp_dir().join(format!(
            "giggles-conformance-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| HarnessError::Fixture)?
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(HarnessError::Fixture),
        }
    }
    Err(HarnessError::Fixture)
}

pub(crate) struct Fixture {
    pub config: Config,
    pub server: Arc<ServerConfig>,
    pub root: PathBuf,
}

impl Fixture {
    pub fn new(
        wrong_name: bool,
        untrusted: bool,
        alpn: Option<&[u8]>,
    ) -> Result<Self, HarnessError> {
        let root = fresh()?;
        let mut params = CertificateParams::new(vec![]).map_err(|_| HarnessError::Fixture)?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let ca_key = KeyPair::generate().map_err(|_| HarnessError::Fixture)?;
        let ca = params
            .self_signed(&ca_key)
            .map_err(|_| HarnessError::Fixture)?;
        let key = KeyPair::generate().map_err(|_| HarnessError::Fixture)?;
        let cert = CertificateParams::new(vec![
            if wrong_name {
                "other.invalid"
            } else {
                "localhost"
            }
            .into(),
        ])
        .map_err(|_| HarnessError::Fixture)?
        .signed_by(&key, &ca, &ca_key)
        .map_err(|_| HarnessError::Fixture)?;
        let client_key = KeyPair::generate().map_err(|_| HarnessError::Fixture)?;
        let client = CertificateParams::new(vec!["client".into()])
            .map_err(|_| HarnessError::Fixture)?
            .signed_by(&client_key, &ca, &ca_key)
            .map_err(|_| HarnessError::Fixture)?;
        let mut roots = RootCertStore::empty();
        roots
            .add(ca.der().clone())
            .map_err(|_| HarnessError::Fixture)?;
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .map_err(|_| HarnessError::Fixture)?;
        let mut server =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|_| HarnessError::Fixture)?
                .with_client_cert_verifier(verifier)
                .with_single_cert(
                    vec![cert.der().clone()],
                    PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
                )
                .map_err(|_| HarnessError::Fixture)?;
        server.alpn_protocols = alpn.into_iter().map(<[u8]>::to_vec).collect();
        let config = Config {
            endpoints: vec!["127.0.0.1:1".parse().map_err(|_| HarnessError::Fixture)?],
            ca: root.join("ca.pem"),
            certificate: root.join("client.pem"),
            key: root.join("client.key"),
            server_name: "localhost".into(),
            limits: Limits {
                connect: Duration::from_millis(500),
                request: Duration::from_millis(500),
                operation: Duration::from_secs(3),
                backoff: Duration::from_millis(1),
                max_attempts: 2,
                max_in_flight: 1,
            },
        };
        let trust = if untrusted {
            let k = KeyPair::generate().map_err(|_| HarnessError::Fixture)?;
            let mut p = CertificateParams::new(vec![]).map_err(|_| HarnessError::Fixture)?;
            p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            p.key_usages = vec![KeyUsagePurpose::KeyCertSign];
            p.self_signed(&k).map_err(|_| HarnessError::Fixture)?.pem()
        } else {
            ca.pem()
        };
        std::fs::write(&config.ca, trust)?;
        std::fs::write(&config.certificate, client.pem())?;
        std::fs::write(&config.key, client_key.serialize_pem())?;
        Ok(Self {
            config,
            server: Arc::new(server),
            root,
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn fixture_uniqueness() {
        assert_ne!(super::fresh().unwrap(), super::fresh().unwrap());
    }

    #[tokio::test]
    async fn server_requires_client_certificate() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let fixture = super::Fixture::new(false, false, Some(b"giggles-native/1")).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            assert!(
                tokio_rustls::TlsAcceptor::from(fixture.server)
                    .accept(tcp)
                    .await
                    .is_err()
            );
        });
        let mut roots = rustls::RootCertStore::empty();
        let pem = std::fs::read(fixture.config.ca).unwrap();
        // Trust only the fixture CA, while deliberately omitting client auth.
        for cert in rustls_pemfile_for_test(&pem) {
            roots.add(cert).unwrap();
        }
        let mut config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        config.alpn_protocols = vec![b"giggles-native/1".to_vec()];
        let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
        if let Ok(mut tls) = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                tcp,
            )
            .await
        {
            let _ = tls.write_all(b"not-authenticated").await;
            let mut byte = [0];
            assert!(tls.read(&mut byte).await.is_err());
        }
        server.await.unwrap();
    }

    fn rustls_pemfile_for_test(pem: &[u8]) -> Vec<rustls::pki_types::CertificateDer<'static>> {
        // rcgen fixture DER can be recovered through the public PEM reader.
        rustls_pemfile::certs(&mut std::io::BufReader::new(pem))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }
}
