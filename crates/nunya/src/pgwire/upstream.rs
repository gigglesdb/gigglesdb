//! Raw upstream PostgreSQL transport and authentication.
use super::codec::{self, Backend, Frontend};
use crate::error::{NyCode, Refusal};
use bytes::{BufMut, BytesMut};
use postgres_protocol::authentication::{
    md5_hash,
    sasl::{ChannelBinding, ScramSha256},
};
use rustls::{
    DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use std::{fmt, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};
use zeroize::Zeroizing;

/// Upstream TLS certificate policy.
#[derive(Debug)]
pub enum UpstreamTls {
    /// Use plain TCP.
    Disable,
    /// Encrypt without authenticating the server, matching libpq sslmode=require.
    Require,
    /// Validate the hostname against only the supplied CA certificates.
    VerifyFull {
        /// PEM certificate authority file.
        ca_file: PathBuf,
    },
}
/// Connection parameters; debug output never exposes connection data.
pub struct UpstreamConfig {
    /// Server hostname.
    pub host: String,
    /// Server TCP port.
    pub port: u16,
    /// Database role.
    pub user: String,
    /// Role password.
    pub password: Zeroizing<String>,
    /// Database name.
    pub dbname: String,
    /// TLS policy.
    pub tls: UpstreamTls,
    /// Startup application name.
    pub application_name: String,
    /// Connection and TLS negotiation deadline in milliseconds.
    pub connect_timeout_ms: u64,
}
impl fmt::Debug for UpstreamConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UpstreamConfig(<redacted>)")
    }
}
trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}
type Stream = Box<dyn Socket>;
fn bad() -> Refusal {
    Refusal::new(
        NyCode::ProtocolNotAllowed,
        "upstream connection or protocol failure",
    )
}

// Require intentionally skips certificate trust and hostname validation, but verifies
// handshake signatures with the configured crypto provider.
#[derive(Debug)]
struct RequireVerifier;
impl ServerCertVerifier for RequireVerifier {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
async fn open(cfg: &UpstreamConfig) -> Result<Stream, Refusal> {
    tokio::time::timeout(Duration::from_millis(cfg.connect_timeout_ms), async {
        let mut tcp = TcpStream::connect((cfg.host.as_str(), cfg.port))
            .await
            .map_err(|_| bad())?;
        if matches!(cfg.tls, UpstreamTls::Disable) {
            return Ok(Box::new(tcp) as Stream);
        }
        tcp.write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
            .await
            .map_err(|_| bad())?;
        tcp.flush().await.map_err(|_| bad())?;
        if tcp.read_u8().await.map_err(|_| bad())? != b'S' {
            return Err(bad());
        }
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| bad())?;
        let tls = match &cfg.tls {
            UpstreamTls::Require => builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(RequireVerifier))
                .with_no_client_auth(),
            UpstreamTls::VerifyFull { ca_file } => {
                let pem = tokio::fs::read(ca_file).await.map_err(|_| bad())?;
                let mut roots = rustls::RootCertStore::empty();
                for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
                    roots.add(cert.map_err(|_| bad())?).map_err(|_| bad())?;
                }
                if roots.is_empty() {
                    return Err(bad());
                }
                builder.with_root_certificates(roots).with_no_client_auth()
            }
            UpstreamTls::Disable => return Err(bad()),
        };
        let name = ServerName::try_from(cfg.host.clone()).map_err(|_| bad())?;
        let stream = tokio_rustls::TlsConnector::from(Arc::new(tls))
            .connect(name, tcp)
            .await
            .map_err(|_| bad())?;
        Ok(Box::new(stream) as Stream)
    })
    .await
    .map_err(|_| bad())?
}
fn quote_identifier(s: &str) -> Result<String, Refusal> {
    if s.contains('\0') {
        return Err(bad());
    }
    Ok(format!("\"{}\"", s.replace('"', "\"\"")))
}
fn settings_query(path: &[String]) -> Result<String, Refusal> {
    let path = path
        .iter()
        .map(|s| quote_identifier(s))
        .collect::<Result<Vec<_>, _>>()?;
    // An empty string selects an empty effective search path.
    let path = if path.is_empty() {
        "''".to_owned()
    } else {
        path.join(", ")
    };
    Ok(format!(
        "SET search_path = {path}; SET standard_conforming_strings = on; SET TimeZone = 'UTC'; SET DateStyle = 'ISO, MDY'; SET IntervalStyle = 'postgres'"
    ))
}
fn sasl_initial(data: &[u8]) -> Result<Vec<u8>, Refusal> {
    let n = i32::try_from(data.len()).map_err(|_| bad())?;
    let mut out = b"SCRAM-SHA-256\0".to_vec();
    out.extend_from_slice(&n.to_be_bytes());
    out.extend_from_slice(data);
    Ok(out)
}
enum Incoming {
    Backend(Backend),
    Cleartext,
    Md5([u8; 4]),
}
/// An authenticated raw upstream connection.
pub struct Upstream {
    stream: Stream,
    buffer: BytesMut,
    key: (i32, Vec<u8>),
    parameters: Vec<(String, String)>,
    status: u8,
}
impl Upstream {
    /// Connect, authenticate, apply the fixed settings, and await readiness.
    pub async fn connect(cfg: &UpstreamConfig, search_path: &[String]) -> Result<Self, Refusal> {
        let query = settings_query(search_path)?;
        let mut startup = BytesMut::new();
        startup.put_u32(0);
        startup.put_u32(196608);
        for (k, v) in [
            ("user", cfg.user.as_str()),
            ("database", cfg.dbname.as_str()),
            ("application_name", cfg.application_name.as_str()),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("TimeZone", "UTC"),
            ("IntervalStyle", "postgres"),
            ("extra_float_digits", "3"),
            ("standard_conforming_strings", "on"),
        ] {
            if v.contains('\0') {
                return Err(bad());
            }
            startup.extend_from_slice(k.as_bytes());
            startup.put_u8(0);
            startup.extend_from_slice(v.as_bytes());
            startup.put_u8(0);
        }
        startup.put_u8(0);
        if startup.len() > codec::MAX_STARTUP {
            return Err(bad());
        }
        let len = startup.len() as u32;
        startup[..4].copy_from_slice(&len.to_be_bytes());
        let mut this = Self {
            stream: open(cfg).await?,
            buffer: BytesMut::new(),
            key: (0, Vec::new()),
            parameters: Vec::new(),
            status: b'I',
        };
        this.stream.write_all(&startup).await.map_err(|_| bad())?;
        this.stream.flush().await.map_err(|_| bad())?;
        let mut scram = None;
        let mut phase = 0u8;
        loop {
            match this.incoming().await? {
                Incoming::Backend(Backend::AuthenticationOk)
                    if phase == 0 || phase == 1 || phase == 4 =>
                {
                    break;
                }
                Incoming::Cleartext if phase == 0 && !matches!(cfg.tls, UpstreamTls::Disable) => {
                    if cfg.password.contains('\0') {
                        return Err(bad());
                    }
                    let mut p = cfg.password.as_bytes().to_vec();
                    p.push(0);
                    this.send(&[Frontend::Password(p)]).await?;
                    phase = 1;
                }
                Incoming::Md5(salt) if phase == 0 => {
                    let mut p =
                        md5_hash(cfg.user.as_bytes(), cfg.password.as_bytes(), salt).into_bytes();
                    p.push(0);
                    this.send(&[Frontend::Password(p)]).await?;
                    phase = 1;
                }
                Incoming::Backend(Backend::AuthenticationSasl(mechanisms))
                    if phase == 0 && mechanisms.iter().any(|s| s == "SCRAM-SHA-256") =>
                {
                    let s =
                        ScramSha256::new(cfg.password.as_bytes(), ChannelBinding::unsupported());
                    this.send(&[Frontend::Password(sasl_initial(s.message())?)])
                        .await?;
                    scram = Some(s);
                    phase = 2;
                }
                Incoming::Backend(Backend::AuthenticationSaslContinue(data)) if phase == 2 => {
                    let s = scram.as_mut().ok_or_else(bad)?;
                    s.update(&data).map_err(|_| bad())?;
                    this.send(&[Frontend::Password(s.message().to_vec())])
                        .await?;
                    phase = 3;
                }
                Incoming::Backend(Backend::AuthenticationSaslFinal(data)) if phase == 3 => {
                    scram
                        .as_mut()
                        .ok_or_else(bad)?
                        .finish(&data)
                        .map_err(|_| bad())?;
                    phase = 4;
                }
                _ => return Err(bad()),
            }
        }
        loop {
            match this.recv().await? {
                Backend::ParameterStatus(_, _)
                | Backend::BackendKeyData { .. }
                | Backend::NoticeResponse(_) => {}
                Backend::ReadyForQuery(b'I') => break,
                _ => return Err(bad()),
            }
        }
        this.send(&[Frontend::Query(query)]).await?;
        loop {
            match this.recv().await? {
                Backend::CommandComplete(_)
                | Backend::ParameterStatus(_, _)
                | Backend::NoticeResponse(_) => {}
                Backend::ReadyForQuery(b'I') => break,
                _ => return Err(bad()),
            }
        }
        Ok(this)
    }
    /// Encode, write, and flush frontend messages in order.
    pub async fn send(&mut self, msgs: &[Frontend]) -> Result<(), Refusal> {
        let mut out = BytesMut::new();
        for msg in msgs {
            if matches!(msg, Frontend::Refused(_)) {
                return Err(bad());
            }
            codec::encode_frontend(msg, &mut out);
        }
        self.stream.write_all(&out).await.map_err(|_| bad())?;
        self.stream.flush().await.map_err(|_| bad())
    }
    async fn incoming(&mut self) -> Result<Incoming, Refusal> {
        loop {
            if self.buffer.len() >= 5 {
                let n =
                    u32::from_be_bytes(self.buffer[1..5].try_into().map_err(|_| bad())?) as usize;
                if !(4..=codec::MAX_MESSAGE).contains(&n) {
                    return Err(bad());
                }
                if self.buffer.len() > n {
                    if self.buffer[0] == b'R' && n >= 8 {
                        let code =
                            u32::from_be_bytes(self.buffer[5..9].try_into().map_err(|_| bad())?);
                        match code {
                            3 if n == 8 => {
                                let _ = self.buffer.split_to(n + 1);
                                return Ok(Incoming::Cleartext);
                            }
                            5 if n == 12 => {
                                let salt = self.buffer[9..13].try_into().map_err(|_| bad())?;
                                let _ = self.buffer.split_to(n + 1);
                                return Ok(Incoming::Md5(salt));
                            }
                            0 | 10 | 11 | 12 => {}
                            _ => return Err(bad()),
                        }
                    }
                    let msg = codec::decode_backend(&mut self.buffer)
                        .map_err(|_| bad())?
                        .ok_or_else(bad)?;
                    if matches!(msg, Backend::Refused(_)) {
                        return Err(bad());
                    }
                    return Ok(Incoming::Backend(msg));
                }
            }
            if self
                .stream
                .read_buf(&mut self.buffer)
                .await
                .map_err(|_| bad())?
                == 0
            {
                return Err(bad());
            }
        }
    }
    /// Receive one backend message and record connection state changes.
    pub async fn recv(&mut self) -> Result<Backend, Refusal> {
        let Incoming::Backend(msg) = self.incoming().await? else {
            return Err(bad());
        };
        match &msg {
            Backend::ParameterStatus(k, v) => {
                if let Some(entry) = self.parameters.iter_mut().find(|(name, _)| name == k) {
                    entry.1.clone_from(v);
                } else {
                    self.parameters.push((k.clone(), v.clone()));
                }
            }
            Backend::BackendKeyData { pid, secret } => self.key = (*pid, secret.clone()),
            Backend::ReadyForQuery(status) => self.status = *status,
            Backend::ErrorResponse(fields) => {
                // Prefer PostgreSQL's non-localized severity. Terminating errors
                // leave an in-flight write uncertain, just like transport loss.
                let severity = fields.iter().find(|(k, _)| *k == b'V')
                    .or_else(|| fields.iter().find(|(k, _)| *k == b'S'))
                    .map(|(_, value)| value.as_str()).unwrap_or("ERROR");
                if matches!(severity, "FATAL" | "PANIC") {
                    return Err(bad());
                }
                // Ordinary statement errors pass through; connect's startup
                // loops still reject them before exposing the connection.
            }
            _ => {}
        }
        Ok(msg)
    }
    /// Return the backend cancellation credentials.
    pub fn backend_key(&self) -> (i32, Vec<u8>) {
        self.key.clone()
    }
    /// Return current server parameter values.
    pub fn parameter_statuses(&self) -> &[(String, String)] {
        &self.parameters
    }
    /// Return the most recently observed transaction status.
    pub fn transaction_status(&self) -> u8 {
        self.status
    }
}
/// Send a cancellation packet over a fresh connection using the same TLS policy.
pub async fn send_cancel(cfg: &UpstreamConfig, pid: i32, secret: &[u8]) -> Result<(), Refusal> {
    if !(4..=256).contains(&secret.len()) {
        return Err(bad());
    }
    let mut stream = open(cfg).await?;
    let mut out = BytesMut::new();
    out.put_u32((12 + secret.len()) as u32);
    out.put_u32(80877102);
    out.put_i32(pid);
    out.extend_from_slice(secret);
    stream.write_all(&out).await.map_err(|_| bad())?;
    stream.flush().await.map_err(|_| bad())?;
    stream.shutdown().await.map_err(|_| bad())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identifier_quoting() {
        assert_eq!(quote_identifier("a\"b").unwrap(), "\"a\"\"b\"");
        assert_eq!(
            quote_identifier("\u{65e5}\u{672c}\u{8a9e}").unwrap(),
            "\"\u{65e5}\u{672c}\u{8a9e}\""
        );
        assert!(quote_identifier("a\0b").is_err());
    }
    #[test]
    fn exact_settings_query() {
        assert_eq!(
            settings_query(&["public".into(), "a\"b".into()]).unwrap(),
            "SET search_path = \"public\", \"a\"\"b\"; SET standard_conforming_strings = on; SET TimeZone = 'UTC'; SET DateStyle = 'ISO, MDY'; SET IntervalStyle = 'postgres'"
        );
        assert!(settings_query(&["x\0y".into()]).is_err());
        assert!(
            settings_query(&[])
                .unwrap()
                .eq("SET search_path = ''; SET standard_conforming_strings = on; SET TimeZone = 'UTC'; SET DateStyle = 'ISO, MDY'; SET IntervalStyle = 'postgres'")
        );
    }
    #[test]
    fn sasl_initial_payload() {
        assert_eq!(
            sasl_initial(b"n,,test").unwrap(),
            b"SCRAM-SHA-256\0\0\0\0\x07n,,test"
        );
    }
    #[test]
    fn debug_redaction() {
        let cfg = UpstreamConfig {
            host: "secret-host".into(),
            port: 5432,
            user: "role".into(),
            password: Zeroizing::new("secret-password".into()),
            dbname: "db".into(),
            tls: UpstreamTls::Disable,
            application_name: "nunya".into(),
            connect_timeout_ms: 1000,
        };
        assert_eq!(format!("{cfg:?}"), "UpstreamConfig(<redacted>)");
    }
}
