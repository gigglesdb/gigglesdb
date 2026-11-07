"""Materialize S2's own source from S1's proven connection/decoder; no source deletion."""
import pathlib
ROOT = pathlib.Path(__file__).resolve().parent
s1 = ROOT.parent / 's1-replication'
source = (s1/'src/main.rs').read_text()
source = source[:source.index('async fn toxi(')]
source = source.replace('use tokio_rustls::{TlsConnector, client::TlsStream};', 'use tokio_rustls::TlsConnector;\ntrait Wire: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}\nimpl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Wire for T {}')
source = source.replace('io: TlsStream<TcpStream>', 'io: Box<dyn Wire>')
start = source.index('        let mut b = BytesMut::new();', source.index('impl Conn'))
stop = source.index('        let mut c = Self {', start)
source = source[:start] + '''        let mut b = BytesMut::new();
        let plain = env::var("S2_PLAIN_PORTS").unwrap_or_default().split(',').any(|p| p == port.to_string());
        let (io, tls): (Box<dyn Wire>, String) = if plain {
            (Box::new(tcp), "pool-client-plaintext-on-loopback".into())
        } else {
            frontend::ssl_request(&mut b);
            tcp.write_all(&b).await?;
            ensure!(tcp.read_u8().await? == b'S', "server refused TLS");
            let cert = File::open(env::var("S1_CERT")?)?;
            let mut roots = RootCertStore::empty();
            for c in rustls_pemfile::certs(&mut BufReader::new(cert)) { roots.add(c?)?; }
            let cfg = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
            let io = TlsConnector::from(Arc::new(cfg)).connect(ServerName::try_from("localhost")?, tcp).await?;
            let tls = format!("{:?}/{:?}", io.get_ref().1.protocol_version(), io.get_ref().1.negotiated_cipher_suite());
            (Box::new(io), tls)
        };
''' + source[stop:]
source = source.replace("b'E' => error = Some(String::from_utf8_lossy(&p).into_owned()),", "b'E' => { let msg = String::from_utf8_lossy(&p).into_owned(); if msg.contains(\"FATAL\") { bail!(\"SQL {q}: {msg}\"); } error = Some(msg); },")
manifest = (s1/'Cargo.toml').read_text().replace('name = "s1-replication"', 'name = "s2-fencing"')
(ROOT/'src').mkdir(exist_ok=True)
(ROOT/'src/main.rs').write_text(source + (ROOT/'s2.rs').read_text())
(ROOT/'Cargo.toml').write_text(manifest)
(ROOT/'Cargo.lock').write_text((s1/'Cargo.lock').read_text().replace('name = "s1-replication"', 'name = "s2-fencing"'))
