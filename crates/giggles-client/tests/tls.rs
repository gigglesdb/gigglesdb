use giggles_client::{Client, Config, Limits, Operation, OperationId, Outcome, UnknownReason};
use giggles_consensus::native;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, KeyUsagePurpose};
use rustls::{RootCertStore, ServerConfig, pki_types::PrivatePkcs8KeyDer};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_rustls::TlsAcceptor;
static NEXT: AtomicU64 = AtomicU64::new(0);
fn fresh() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "giggles-client-tls-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}
struct Fixture {
    config: Config,
    server: Arc<ServerConfig>,
}
fn fixture(name: &str, expired: bool) -> Fixture {
    let root = fresh();
    let mut params = CertificateParams::new(vec![]).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&ca_key).unwrap();
    let key = KeyPair::generate().unwrap();
    let mut leaf = CertificateParams::new(vec![name.into()]).unwrap();
    if expired {
        leaf.not_before = rcgen::date_time_ymd(2010, 1, 1);
        leaf.not_after = rcgen::date_time_ymd(2011, 1, 1);
    }
    let cert = leaf.signed_by(&key, &ca, &ca_key).unwrap();
    // Client certificate uses separate valid leaf even when the server is expired.
    let client_key = KeyPair::generate().unwrap();
    let client = CertificateParams::new(vec!["client".into()])
        .unwrap()
        .signed_by(&client_key, &ca, &ca_key)
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(ca.der().clone()).unwrap();
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .unwrap();
    let mut server =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![cert.der().clone()],
                PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            )
            .unwrap();
    server.alpn_protocols = vec![b"giggles-native/1".to_vec()];
    let config = Config {
        endpoints: vec!["127.0.0.1:1".parse().unwrap()],
        ca: root.join("ca.pem"),
        certificate: root.join("client.pem"),
        key: root.join("client.key"),
        server_name: "localhost".into(),
        limits: Limits {
            connect: Duration::from_secs(1),
            request: Duration::from_secs(1),
            operation: Duration::from_secs(2),
            ..Limits::default()
        },
    };
    std::fs::write(&config.ca, ca.pem()).unwrap();
    std::fs::write(&config.certificate, client.pem()).unwrap();
    std::fs::write(&config.key, client_key.serialize_pem()).unwrap();
    Fixture {
        config,
        server: Arc::new(server),
    }
}
async fn exchange(mode: &str, execute: bool) -> Outcome {
    let mut f = fixture("localhost", false);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    f.config.endpoints = vec![listener.local_addr().unwrap()];
    let mode = mode.to_owned();
    let task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut tls = TlsAcceptor::from(f.server).accept(tcp).await.unwrap();
        let req = native::parse(&native::read_frame(&mut tls).await.unwrap()).unwrap();
        match mode.as_str() {
            "zero" => {
                tls.write_u32(0).await.unwrap();
            }
            "oversize" => {
                tls.write_u32(u32::MAX).await.unwrap();
            }
            "truncated" => {
                tls.write_u32(100).await.unwrap();
                tls.write_all(b"{}").await.unwrap();
            }
            "lost" => {}
            "stall" => {
                let _ = tls.read_u8().await;
            }
            _ => {
                let mut response = serde_json::json!({"version":1,"id":req.id,"error":null,"reply":{"version":1,"code":0,"data":{"version":1,"code":"committed","operation":77}}});
                match mode.as_str() {
                    "correlation" => response["id"] = serde_json::json!(req.id + 1),
                    "version" => response["version"] = serde_json::json!(2),
                    "both" => response["error"] = serde_json::json!("indeterminate"),
                    "neither" => response["reply"] = serde_json::Value::Null,
                    "unknown_field" => response["extra"] = serde_json::json!(true),
                    "reply_version" => response["reply"]["version"] = serde_json::json!(2),
                    _ => {}
                }
                native::write_frame(&mut tls, &serde_json::to_vec(&response).unwrap())
                    .await
                    .unwrap();
            }
        }
    });
    let client = Client::new(f.config).unwrap();
    let op = if execute {
        Operation::execute(
            1,
            giggles_client::Request {
                operation: OperationId(77),
                selection: giggles_core::Selection::Keys(vec![]),
                column: giggles_core::Column::Status,
                after: giggles_core::Status::Queued,
            },
            None,
            1000,
        )
        .unwrap()
    } else {
        Operation::status(1, OperationId(77), 1000).unwrap()
    };
    let result = client.send(&op).await;
    task.await.unwrap();
    result
}
#[tokio::test]
async fn valid_mtls_framing() {
    assert!(matches!(exchange("valid",false).await,Outcome::Recorded(r) if r.is_committed()));
}
#[tokio::test]
async fn zero_oversize_and_truncated_frames_are_bounded_unknown() {
    for mode in ["zero", "oversize", "truncated", "lost"] {
        assert!(
            matches!(
                exchange(mode, true).await,
                Outcome::Unknown {
                    operation_id: OperationId(77),
                    ..
                }
            ),
            "{mode}"
        );
    }
}
#[tokio::test]
async fn response_correlation_shape_version_and_grammar() {
    for mode in [
        "correlation",
        "version",
        "both",
        "neither",
        "unknown_field",
        "reply_version",
    ] {
        assert!(
            matches!(
                exchange(mode, true).await,
                Outcome::Unknown {
                    reason: UnknownReason::Protocol,
                    ..
                }
            ),
            "{mode}"
        );
    }
}
#[tokio::test]
async fn stalled_reply_consumes_request_deadline() {
    assert!(matches!(
        exchange("stall", true).await,
        Outcome::Unknown {
            reason: UnknownReason::Timeout,
            ..
        }
    ));
}
#[tokio::test]
async fn wrong_name_expired_and_untrusted_server_no_fallback() {
    for mode in ["name", "expired", "untrusted"] {
        let mut f = fixture(
            if mode == "name" {
                "different.invalid"
            } else {
                "localhost"
            },
            mode == "expired",
        );
        if mode == "untrusted" {
            f.config.ca = fixture("localhost", false).config.ca;
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        f.config.endpoints = vec![listener.local_addr().unwrap()];
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            assert!(TlsAcceptor::from(f.server).accept(tcp).await.is_err());
        });
        let c = Client::new(f.config).unwrap();
        assert!(
            matches!(
                c.status(1, OperationId(77), 1000).await.unwrap(),
                Outcome::TlsRejected
            ),
            "{mode}"
        );
        task.await.unwrap();
    }
}
#[test]
fn missing_bad_or_oversize_credentials_fail_locally() {
    let mut f = fixture("localhost", false);
    f.config.key = f.config.key.with_extension("missing");
    assert!(matches!(
        Client::new(f.config),
        Err(giggles_client::ClientError::Credentials)
    ));
    let f = fixture("localhost", false);
    std::fs::write(&f.config.ca, b"not PEM").unwrap();
    assert!(matches!(
        Client::new(f.config),
        Err(giggles_client::ClientError::Credentials)
    ));
    let f = fixture("localhost", false);
    std::fs::write(&f.config.key, vec![b'x'; 1024 * 1024 + 1]).unwrap();
    assert!(matches!(
        Client::new(f.config),
        Err(giggles_client::ClientError::Credentials)
    ));
}
