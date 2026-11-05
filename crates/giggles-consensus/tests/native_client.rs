mod common;
use giggles_consensus::{TaskAction, native};
use giggles_core::{OperationId, local::Reply};
use rustls::{ServerConfig, pki_types::PrivatePkcs8KeyDer};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

// Script TLS endpoint outcomes to test the real client's cross-attempt contract.
// Server-side client authentication is covered by the three-process suite.
#[tokio::test]
async fn explicit_indeterminate_survives_later_refusals_but_success_resolves() {
    for outcomes in [
        vec!["indeterminate", "unavailable"],
        vec!["unavailable", "indeterminate", "capacity"],
        vec!["indeterminate", "success"],
        vec!["unavailable", "capacity"],
    ] {
        let root = common::fresh("client-outcomes");
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = root.join("certificate.pem");
        let key = root.join("key.pem");
        std::fs::write(&cert, generated.cert.pem()).unwrap();
        std::fs::write(&key, generated.key_pair.serialize_pem()).unwrap();
        let server = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![generated.cert.der().clone()],
                PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der()).into(),
            )
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server));
        let mut addresses = Vec::new();
        let mut tasks = Vec::new();
        for outcome in &outcomes {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(listener.local_addr().unwrap().to_string());
            let acceptor = acceptor.clone();
            let outcome = outcome.to_string();
            tasks.push(tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut tls = acceptor.accept(tcp).await.unwrap();
                let bytes = native::read_frame(&mut tls).await.unwrap();
                let request = native::parse(&bytes).unwrap();
                assert_eq!(request.id, 44);
                let response = native::Response {
                    version: 1,
                    id: request.id,
                    error: (outcome != "success").then_some(outcome.clone()),
                    reply: (outcome == "success")
                        .then(|| Reply::ok(serde_json::json!({"code":"absent"}))),
                };
                native::write_frame(&mut tls, &serde_json::to_vec(&response).unwrap())
                    .await
                    .unwrap();
            }));
        }
        let config = native::ClientConfiguration {
            addresses,
            certificate: cert.clone(),
            key,
            ca: cert,
            server_name: "localhost".into(),
        };
        let result = native::call(
            &config,
            &native::Request {
                version: 1,
                id: 44,
                deadline_ms: 5000,
                action: TaskAction::Status(OperationId(77)),
            },
        )
        .await;
        for task in tasks {
            task.await.unwrap();
        }
        std::fs::write(
            root.join("outcome.txt"),
            format!("{outcomes:?}: {result:?}\n"),
        )
        .unwrap();
        if outcomes.contains(&"success") {
            assert!(result.unwrap().reply.is_some());
        } else if outcomes.contains(&"indeterminate") {
            assert_eq!(result.unwrap().error.as_deref(), Some("indeterminate"));
        } else {
            assert_eq!(result.unwrap().error.as_deref(), Some("capacity"));
        }
    }
}
