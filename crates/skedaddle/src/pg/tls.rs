//! Verified TLS for every tokio-postgres connection.
//!
//! Channel binding is intentionally unavailable: rustls exposes the peer DER
//! certificate, but not its signature hash algorithm. Without an X.509 parser
//! we cannot correctly implement RFC 5929 for arbitrary CA-issued certificates.
//! `channel_binding=require` therefore fails closed; the default `prefer` uses
//! SCRAM-SHA-256 over certificate- and hostname-verified TLS. No verifier bypass
//! or automatic plaintext fallback is provided.

use std::{
    future::Future,
    io::{self, BufReader},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_postgres::{
    Client,
    config::SslMode,
    tls::{ChannelBinding, MakeTlsConnect, TlsConnect},
};
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::security::{TlsPolicy, WarningItem};

/// Explicit trust configuration. The CA file is never inferred from the DSN or
/// process environment. Hostname verification uses each configured PG host.
#[derive(Clone)]
pub struct Config {
    pub postgres: tokio_postgres::Config,
    pub ca_pem: PathBuf,
}

impl Config {
    pub fn new(postgres: tokio_postgres::Config, ca_pem: impl AsRef<Path>) -> Self {
        Self {
            postgres,
            ca_pem: ca_pem.as_ref().to_owned(),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Trust,
    Connect(tokio_postgres::Error),
    Policy(crate::security::SecurityError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Trust => "TLS CA configuration could not be loaded",
            Self::Connect(_) => "PostgreSQL connection failed (details redacted)",
            Self::Policy(_) => "TLS transport policy rejected the connection",
        })
    }
}
impl std::error::Error for Error {}

/// Evidence records an actual completed TLS handshake, never requested sslmode.
#[derive(Clone, Debug)]
pub struct TransportEvidence {
    encrypted: bool,
    pub warnings: Vec<WarningItem>,
}
impl TransportEvidence {
    pub fn encrypted(&self) -> bool {
        self.encrypted
    }
}

pub type Driver = Pin<Box<dyn Future<Output = Result<(), tokio_postgres::Error>> + Send>>;

pub async fn connect(
    config: &Config,
    policy: &TlsPolicy,
) -> Result<(Client, Driver, TransportEvidence), Error> {
    let mut postgres = config.postgres.clone();
    match policy {
        TlsPolicy::Required => {
            // Override even sslmode=disable/prefer supplied in the DSN.
            postgres.ssl_mode(SslMode::Require);
            let connector = MakeRustlsConnect::from_ca_pem(&config.ca_pem)?;
            let encrypted = connector.encrypted.clone();
            let (client, driver) = postgres.connect(connector).await.map_err(Error::Connect)?;
            let encrypted = encrypted.load(Ordering::Acquire);
            let transport = evidence(policy, encrypted)?;
            Ok((client, Box::pin(driver), transport))
        }
        TlsPolicy::DisableForLocalTest => {
            let transport = evidence(policy, false)?;
            postgres.ssl_mode(SslMode::Disable);
            let (client, driver) = postgres
                .connect(tokio_postgres::NoTls)
                .await
                .map_err(Error::Connect)?;
            Ok((client, Box::pin(driver), transport))
        }
    }
}

#[derive(Clone)]
pub struct MakeRustlsConnect {
    connector: TlsConnector,
    encrypted: Arc<AtomicBool>,
}
impl MakeRustlsConnect {
    pub fn from_ca_pem(path: impl AsRef<Path>) -> Result<Self, Error> {
        let mut roots = RootCertStore::empty();
        let mut pem = BufReader::new(std::fs::File::open(path).map_err(|_| Error::Trust)?);
        for certificate in rustls_pemfile::certs(&mut pem) {
            roots
                .add(certificate.map_err(|_| Error::Trust)?)
                .map_err(|_| Error::Trust)?;
        }
        if roots.is_empty() {
            return Err(Error::Trust);
        }
        let mut config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|_| Error::Trust)?
                .with_root_certificates(roots)
                .with_no_client_auth();
        config.resumption = rustls::client::Resumption::disabled();
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            encrypted: Arc::new(AtomicBool::new(false)),
        })
    }
}

pub struct RustlsConnect {
    connector: TlsConnector,
    name: ServerName<'static>,
    encrypted: Arc<AtomicBool>,
}
pub struct Stream<S>(TlsStream<S>);

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> MakeTlsConnect<S> for MakeRustlsConnect {
    type Stream = Stream<S>;
    type TlsConnect = RustlsConnect;
    type Error = io::Error;
    fn make_tls_connect(&mut self, domain: &str) -> io::Result<RustlsConnect> {
        let name = ServerName::try_from(domain.to_owned())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid TLS hostname"))?;
        Ok(RustlsConnect {
            connector: self.connector.clone(),
            name,
            encrypted: self.encrypted.clone(),
        })
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> TlsConnect<S> for RustlsConnect {
    type Stream = Stream<S>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Stream<S>>> + Send>>;
    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move {
            let stream = self.connector.connect(self.name, stream).await?;
            self.encrypted.store(true, Ordering::Release);
            Ok(Stream(stream))
        })
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> tokio_postgres::tls::TlsStream for Stream<S> {
    fn channel_binding(&self) -> ChannelBinding {
        ChannelBinding::none()
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Stream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Stream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

fn evidence(policy: &TlsPolicy, encrypted: bool) -> Result<TransportEvidence, Error> {
    Ok(TransportEvidence {
        encrypted,
        warnings: policy.check(encrypted).map_err(Error::Policy)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // Public, disposable test certificate/key; generated solely for TLS fault injection.
    const CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDHDCCAgSgAwIBAgIUJSoBuUMVVCyHyvlX7srpndLVh9MwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MTEwNzEyNDYwNloXDTM2MTEw
NDEyNDYwNlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAvC95kBEwBv0PRf3ppbFsFtla9zJqZA/zOMEMv2GMpVNR
poAQABODiZoNJj9wzQDvdOe9cxaJ8OJfwpCu3+kXpuPJ7w95pVJOhM7bCS8Sm1F7
VCd+ORIVr1iKZHnJcNPEeP7442Fb9ZBkx/b5s+QjmF0vVQ6pVf8uciKFES8lAXLk
d5n4VJgVQ9wJb9Ay3IRb8wegauTqbqwu6QqWp7DVwSSUQ4Nsktc/Uc9IVo5fiZ0Q
lMXb/QyJzwSqU6Ufiz+ATDRL5AuV8coldHJ+PtVlZUUIm2IBrjm7ycvvrmKoIGqU
RQC+V7HYNLykYJMO/WG2D96WNNl2xqfNb5Je9Top5wIDAQABo2YwZDAdBgNVHQ4E
FgQUhHwGvfGazQ1dLL+5Onpf9gWNenMwHwYDVR0jBBgwFoAUhHwGvfGazQ1dLL+5
Onpf9gWNenMwFAYDVR0RBA0wC4IJbG9jYWxob3N0MAwGA1UdEwEB/wQCMAAwDQYJ
KoZIhvcNAQELBQADggEBAFRKnlCnUjcQr0eV1DTfE/PxV/49/CFxH0wE2rBe63ac
0eZGKBoWOS0e9Hr1qdO1HKqUWiErsC1vARAdQdr2SiG67J2JPXmQtW4jUdg66hGU
QWLIx/W6ALSgZcdYtoln3jQmSQpPSXjbz+93eEe+N7onL32N67Z1xo6mRgz6jTQA
RduqcHb6JgXfmSLmTNi9pOjTRUTkJdS/YmTiE7oFS06UKwnOYBJ+Lr1WfBuv69ud
7inUSS6UWfwk6BdJSx33Lby08xS1HvSCw5qmstQRkSIwTJv1G7qg/cFWTJrp3Gzu
0sg81NPcDPvxMNwTzaQ7uoKHNDjyrgDuUyC5gj3KwEU=
-----END CERTIFICATE-----
"#;
    const KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC8L3mQETAG/Q9F
/emlsWwW2Vr3MmpkD/M4wQy/YYylU1GmgBAAE4OJmg0mP3DNAO90571zFonw4l/C
kK7f6Rem48nvD3mlUk6EztsJLxKbUXtUJ345EhWvWIpkeclw08R4/vjjYVv1kGTH
9vmz5COYXS9VDqlV/y5yIoURLyUBcuR3mfhUmBVD3Alv0DLchFvzB6Bq5OpurC7p
CpansNXBJJRDg2yS1z9Rz0hWjl+JnRCUxdv9DInPBKpTpR+LP4BMNEvkC5XxyiV0
cn4+1WVlRQibYgGuObvJy++uYqggapRFAL5Xsdg0vKRgkw79YbYP3pY02XbGp81v
kl71OinnAgMBAAECggEASBQvDZ1nteWLyhU9Pve8HOxXyUMIORda46wdw9gLVO+a
4JlAh6bxxOy6iKHYBM2M6kww5joFz7wFTbK5KI5RPt0U5XoZ79acuIW6o8Ph7pfB
EzSlRhrFL4pMR7vWCl8ya93NWtpZrK8HywewENUbRSq8DGo3NfctxQ2vxis2k1zT
J4y4ywhQjVDKLGvn8LpB0+84Tvzar7v0mptMrVCLsBnzmFJRNZuP37AkmeGPy4Vv
q3NU9D4NCmYEk8ftIWmJNl6G9Y356FiqY8Bg63FT1uudMIlFwXY2qcBeEZo1/i2Z
c9pqKIDFd/9smR9q8vky4T7woYXuVRUBmKlL4werUQKBgQD15OOjQvAH1K7Tsq5Y
L0FHseXgbBhyZBSBkl91ghCsRxnp+SOjULuejiK1bUZQxD6pwYCbc6IgpeTGA28k
AloR9Go39nbX4uONX8wbSVukKOrlQkdB1iRMTfmQMbKobQXdQBEWPyxAd+d5nanB
BexJy1v5DOjFqTGypkA2mDWvDQKBgQDD62tJ/vtrGZ5q0btwMvs3MTD2qBSDf5T1
/HRZ3TmYxn4SwpLDM373MXMNT+Vhoc369UMBG20Mnak2owRELKe5ceEnSKSjEenk
De57ekbKZDFNd0RA/S+P/xr3i512WEj/3Jzyk1JGU4PWyEFQ4R9pQ8cbh52jPCUU
6mAAYeBfwwKBgQDIozpHReAbU6RPYpW1qOFIQdVDWJBeDmFZFHlFpZo7KEzxx/MX
WFGu32Vcykw8rNRAqX5p2WFp6FMSa0ztgenCir986GMyZpSsD4W4ohPfj++Sn5EL
HFND28ANsy+HFGZbkOIgV+2gsPW0GzyfHlkeIYvrXGg2mIbPsa3lni73pQKBgQCu
vs3lJA5vylvn/sVmH7K92ZxIhEY3u5XLia7lcFItF+3fIbjxbn7yUCLEXtzDJiSo
kBWBaUrsk7sXxnLJs+KtnwsplLtzz0iEZoCtlbSKVK883wH7zVec4fbVkKuToM8L
l2RNYGVj4J2Y9uh7ETCJHTD8Nwu9Sqnwa9PQw3vzLQKBgCus50b5J3wVRqRFqSJo
Ol1L1UCK5fVey31nKJVeEbVU0+dKb+Z0FXJZRPxFd83LtvFcduMLZYaPbE0WS073
hDWWrYWQaK8hzBNCN2GrMlFEgg36b6jQs+9+0r9DSsTIf6/Bo17Yl+wBPHK+1o69
gNWyYWaNvXRlDkjhHhcYHLyK
-----END PRIVATE KEY-----
"#;
    const OTHER_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDATCCAemgAwIBAgIUM8YqaicNRoMRIX2hXu3r5oaXmewwDQYJKoZIhvcNAQEL
BQAwEDEOMAwGA1UEAwwFb3RoZXIwHhcNMjYxMTA3MTI0ODA2WhcNMzYxMTA0MTI0
ODA2WjAQMQ4wDAYDVQQDDAVvdGhlcjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBAKBQqJoPhRPDwsmuODS4b1WY5fVHHUs16lN6I61XChGRegy2wg7gglpQ
LN1iCofdRf83R81V8k6z1p9J1BeDG8+weiHBY2OlSub3BHCHnLFCK6a3XtkFBoQk
RX8ZI7c05aLO+EFagTF6VOl3lxeI2x44RiOifhwaUwOhkNu9ovO8JdlkYNywOd8F
/ze/DvvZ0/4Sb1amO8cneIva4U9zftLSF5Y91QoTvhtz36OhEv08KMZgL+IhB5YX
fg3h69wvbzOYcakvt6UvC+0lMRaWWOTF9BKqfKXha0v+EhQcj9rTMgxyvciLX8hD
kreIZkALmMZvu4jqKTfdWs0O8zVN6usCAwEAAaNTMFEwHQYDVR0OBBYEFLIcu7Fj
Ox2LWk9Y+NE3SWtcIC/IMB8GA1UdIwQYMBaAFLIcu7FjOx2LWk9Y+NE3SWtcIC/I
MA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAHgPP6RH6z9tOy43
a9tBW7fJbxYd01nVT1hrZ/m2MruIzvWuhtnNpy8WLEO/f5ZsylXc5IPbtz96He9r
R6YNeMbmMUY60OBbRiHiaP/zMkICdhu4J3YXC2hCZ0LONR98PgVtDJo+78LYZ/3b
v4W2CuM9/3VY3ZWSHH3Dfq0g51af/bTmWrnN7+in4lO973v2cD4c2pldpuq+SPR2
XWFx2StDWSjasvNr6dAm/r1bmXRNAOGP0LOsAIuwbxlSrZGcwvn0sE9fATTD0yWv
HJppNyriIi5SvbO0AiWEP2j2068/qyixGlV52NCIR6WAT1q7ODkFrJQ6og3iQl+U
XvsIkJU=
-----END CERTIFICATE-----
"#;

    fn ca_file(pem: &str) -> PathBuf {
        let root = crate::test_root::scratch_root();
        crate::test_root::assert_under_work_root(&root);
        let path = root.join(format!("tls-{}.pem", uuid::Uuid::new_v4()));
        std::fs::write(&path, pem).unwrap();
        path
    }

    #[tokio::test]
    async fn require_refuses_plaintext_before_startup_or_credentials() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let mut postgres = tokio_postgres::Config::new();
        postgres
            .host("127.0.0.1")
            .port(listener.local_addr().unwrap().port())
            .user("never-sent-user")
            .password("never-sent-password")
            .ssl_mode(SslMode::Disable);
        let config = Config::new(postgres, ca_file(CERT));
        let policy = TlsPolicy::default();
        let (result, ()) = tokio::join!(connect(&config, &policy), async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 8];
            socket.read_exact(&mut request).await.unwrap();
            assert_eq!(request, [0, 0, 0, 8, 4, 210, 22, 47]);
            socket.write_all(b"N").await.unwrap();
            let mut unexpected = [0; 1];
            assert_eq!(
                tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    socket.read(&mut unexpected)
                )
                .await
                .unwrap()
                .unwrap(),
                0
            );
        });
        assert!(matches!(result, Err(Error::Connect(_))));
    }

    async fn handshake(
        name: &str,
        ca: &str,
    ) -> (io::Result<Stream<tokio::io::DuplexStream>>, Arc<AtomicBool>) {
        let mut make = MakeRustlsConnect::from_ca_pem(ca_file(ca)).unwrap();
        let encrypted = make.encrypted.clone();
        let connector =
            <MakeRustlsConnect as MakeTlsConnect<tokio::io::DuplexStream>>::make_tls_connect(
                &mut make, name,
            )
            .unwrap();
        let certs = rustls_pemfile::certs(&mut CERT.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = rustls_pemfile::private_key(&mut KEY.as_bytes())
            .unwrap()
            .unwrap();
        let server = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        let (client, server) = tokio::io::duplex(16384);
        let (result, _) = tokio::join!(connector.connect(client), acceptor.accept(server));
        (result, encrypted)
    }

    #[tokio::test]
    async fn wrong_hostname_is_rejected() {
        let (result, encrypted) = handshake("wrong.example", CERT).await;
        let error = result.err().expect("wrong hostname must fail");
        assert!(format!("{error:?}").contains("NotValidForName"));
        assert!(!encrypted.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn untrusted_ca_is_rejected() {
        let (result, encrypted) = handshake("localhost", OTHER_CERT).await;
        let error = result.err().expect("untrusted certificate must fail");
        assert!(format!("{error:?}").contains("UnknownIssuer"));
        assert!(!encrypted.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn evidence_requires_a_completed_verified_handshake() {
        let (result, encrypted) = handshake("localhost", CERT).await;
        assert!(result.is_ok(), "verified TLS handshake failed");
        let transport = evidence(&TlsPolicy::Required, encrypted.load(Ordering::Acquire)).unwrap();
        assert!(transport.encrypted());
        assert!(transport.warnings.is_empty());
        assert!(evidence(&TlsPolicy::Required, false).is_err());
    }

    #[test]
    fn local_test_policy_returns_the_security_warning() {
        let transport = evidence(&TlsPolicy::DisableForLocalTest, false).unwrap();
        assert!(!transport.encrypted());
        assert_eq!(
            transport.warnings,
            TlsPolicy::DisableForLocalTest.check(false).unwrap()
        );
        assert_eq!(transport.warnings[0].code, "plaintext_local_test_only");
    }

    #[tokio::test]
    async fn harness_connection_reports_real_encryption() {
        let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
            use std::io::Write;
            let _ = writeln!(
                std::io::stderr(),
                "SKIP tls real-PG evidence: SKEDADDLE_TEST_CONFIG absent; TLS protocol tests ran, DB qualification NOT RUN"
            );
            return;
        };
        let config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let ca = config["ca_pem"]
            .as_str()
            .or_else(|| config["replication"]["ca_pem"].as_str())
            .expect("harness CA");
        let config = Config::new(
            config["pg"].as_str().expect("harness pg").parse().unwrap(),
            ca,
        );
        let (client, driver, transport) = connect(&config, &TlsPolicy::Required).await.unwrap();
        let driver = tokio::spawn(driver);
        assert!(transport.encrypted());
        assert!(
            client
                .query_one(
                    "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()",
                    &[]
                )
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
        drop(client);
        driver.await.unwrap().unwrap();
    }
}
