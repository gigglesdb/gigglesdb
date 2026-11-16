use giggles_server::{Running, auth::TlsMode, replication::refuse::serve_refusal_bound, tls};
use rustls::{ClientConfig, pki_types::PrivatePkcs8KeyDer};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_postgres::{NoTls, config::SslMode, error::SqlState};
use tokio_postgres_rustls::MakeRustlsConnect;

const HINT: &str = "this node is a replication follower; leader is node 2";

async fn start(tls: Option<Arc<rustls::ServerConfig>>) -> Running {
    serve_refusal_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        tls,
        Arc::new(|| HINT.to_owned()),
        4,
    )
    .await
    .unwrap()
}

fn refusal(error: tokio_postgres::Error) {
    assert_eq!(error.code(), Some(&SqlState::CANNOT_CONNECT_NOW));
    assert!(
        error
            .as_db_error()
            .unwrap()
            .detail()
            .unwrap()
            .contains(HINT)
    );
}

#[tokio::test]
async fn plain() {
    let server = start(None).await;
    let result = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=x dbname=x sslmode=disable",
            server.address.port()
        ),
        NoTls,
    )
    .await;
    refusal(result.err().expect("follower must refuse startup"));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn multi_host() {
    let follower = start(None).await;
    let leader = super::postgresql_network::Fixture::new(TlsMode::Disabled, true, false).await;
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .host("127.0.0.1")
        .port(follower.address.port())
        .port(leader.pg.address.port())
        .user("owner")
        .dbname("postgres")
        .ssl_mode(SslMode::Disable);
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let task = tokio::spawn(connection);
    let rows = client.simple_query("SELECT 1").await.unwrap();
    assert!(
        rows.iter().any(|message| matches!(message, tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0) == Some("1")))
    );
    drop(client);
    task.await.unwrap().unwrap();
    follower.shutdown().await.unwrap();
    leader.stop().await;
}

#[tokio::test]
async fn encrypted() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let server_config = tls::server_optional_client(
        vec![cert.cert.der().clone()],
        PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into(),
        None,
    )
    .unwrap();
    let client_config = ClientConfig::builder()
        .with_root_certificates(tls::roots(vec![cert.cert.der().clone()]).unwrap())
        .with_no_client_auth();
    let server = start(Some(server_config)).await;
    let result = tokio_postgres::Config::new()
        .host("localhost")
        .hostaddr(server.address.ip())
        .port(server.address.port())
        .user("x")
        .dbname("x")
        .ssl_mode(SslMode::Require)
        .connect(MakeRustlsConnect::new(client_config))
        .await;
    refusal(result.err().expect("TLS follower must refuse startup"));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn stop() {
    let mut server = start(None).await;
    let address = server.address;
    let mut idle = TcpStream::connect(address).await.unwrap();
    // A negotiation response proves the connection task is running before shutdown.
    idle.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await.unwrap();
    assert_eq!(idle.read_u8().await.unwrap(), b'N');
    server.request_shutdown();
    server.shutdown().await.unwrap();
    assert!(idle.read_u8().await.is_err());
    assert!(TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn negotiation_limits_and_silent_close() {
    let server = start(None).await;
    for codes in [
        vec![80877103u32, 80877103],
        vec![80877104, 80877104, 80877104],
        vec![80877104, 80877103, 80877104],
        vec![80877102],
        vec![1234],
    ] {
        let mut stream = TcpStream::connect(server.address).await.unwrap();
        for (index, code) in codes.iter().enumerate() {
            stream.write_u32(8).await.unwrap();
            stream.write_u32(*code).await.unwrap();
            if index + 1 < codes.len() {
                assert_eq!(stream.read_u8().await.unwrap(), b'N');
            }
        }
        assert!(stream.read_u8().await.is_err());
    }
    server.shutdown().await.unwrap();
    for limit in [0, 129] {
        let result = serve_refusal_bound(
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            None,
            Arc::new(|| HINT.to_owned()),
            limit,
        )
        .await;
        assert!(result.is_err());
    }
}
