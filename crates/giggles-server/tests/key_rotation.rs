use super::postgresql_network::{Fixture, PASSWORD};
use giggles_server::{auth::TlsMode, tls};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn scram_password_rotation_preserves_authenticated_session() {
    let f = Fixture::new(TlsMode::Disabled, false, false).await;
    let existing = f.plain("owner", Some(PASSWORD)).await;
    existing
        .batch_execute("ALTER ROLE owner PASSWORD 'rotated-password'")
        .await
        .unwrap();
    existing.simple_query("SELECT 1").await.unwrap();
    let error = f
        .config("owner", Some(PASSWORD), false)
        .connect(tokio_postgres::NoTls)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code().unwrap().code(), "28P01");
    let fresh = f.plain("owner", Some("rotated-password")).await;
    fresh.simple_query("SELECT 1").await.unwrap();
    drop((existing, fresh));
    f.stop().await;
}

#[tokio::test]
async fn tls_reload_changes_new_handshakes_and_retains_existing_streams() {
    let dir = std::env::temp_dir().join(format!("key-rotation-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let first = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let second = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate = dir.join("certificate.pem");
    let key = dir.join("key.pem");
    std::fs::write(&certificate, first.cert.pem()).unwrap();
    std::fs::write(&key, first.key_pair.serialize_pem()).unwrap();
    let mut config = tls::server_optional_client(
        tls::certs(&certificate).unwrap(),
        tls::key(&key).unwrap(),
        None,
    )
    .unwrap();
    let reload = tls::ReloadableCertificate::install(&mut config);
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = socket.accept().await.unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(config.clone());
            tokio::spawn(async move {
                let mut stream = acceptor.accept(socket).await.unwrap();
                while let Ok(byte) = stream.read_u8().await {
                    stream.write_u8(byte).await.unwrap();
                }
            });
        }
    });
    let mut roots = rustls::RootCertStore::empty();
    roots.add(first.cert.der().clone()).unwrap();
    roots.add(second.cert.der().clone()).unwrap();
    let client = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let connect = || {
        let client = client.clone();
        async move {
            tokio_rustls::TlsConnector::from(client)
                .connect(
                    rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                    tokio::net::TcpStream::connect(address).await.unwrap(),
                )
                .await
                .unwrap()
        }
    };
    let fingerprint = |s: &tokio_rustls::client::TlsStream<tokio::net::TcpStream>| {
        tls::fingerprint(s.get_ref().1.peer_certificates().unwrap()[0].as_ref())
    };
    let mut existing = connect().await;
    assert_eq!(fingerprint(&existing), tls::fingerprint(first.cert.der()));
    std::fs::write(&certificate, second.cert.pem()).unwrap();
    std::fs::write(&key, second.key_pair.serialize_pem()).unwrap();
    reload.reload(&certificate, &key).unwrap();
    assert_eq!(
        fingerprint(&connect().await),
        tls::fingerprint(second.cert.der())
    );
    existing.write_u8(42).await.unwrap();
    assert_eq!(existing.read_u8().await.unwrap(), 42);
    for bad_key in ["invalid PEM".to_string(), first.key_pair.serialize_pem()] {
        std::fs::write(&key, bad_key).unwrap();
        assert!(reload.reload(&certificate, &key).is_err());
        assert_eq!(
            fingerprint(&connect().await),
            tls::fingerprint(second.cert.der())
        );
    }
    drop(existing);
    server.abort();
    let _ = server.await;
    std::fs::remove_dir_all(dir).unwrap();
}
