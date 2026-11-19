mod api_support;
use api_support::{Fixture, rpc, sql};
use giggles_server::api::{ApiLimits, proto};
use serde_json::{Value, json};

#[tokio::test]
async fn native_postgres_http_and_grpc_share_physical_socket_admission() {
    use giggles_server::{admission::ServiceLimits, serve, serve_pg, tls};
    use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
    use std::{io, net::SocketAddr, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };
    use tokio_rustls::{TlsConnector, client::TlsStream};
    let f = Fixture::with_service_limits(
        ApiLimits::default(),
        ServiceLimits {
            sessions: 1,
            ..ServiceLimits::default()
        },
    )
    .await;
    let config = tls::server(
        vec![f.server.cert.der().clone()],
        PrivatePkcs8KeyDer::from(f.server.key.serialize_der()).into(),
        vec![f.ca.cert.der().clone()],
    )
    .unwrap();
    let native = serve(
        "127.0.0.1:0".parse().unwrap(),
        f.service.clone(),
        config.clone(),
        32,
    )
    .await
    .unwrap();
    let pg = serve_pg(
        "127.0.0.1:0".parse().unwrap(),
        f.service.clone(),
        config,
        32,
    )
    .await
    .unwrap();
    async fn connect(
        f: &Fixture,
        address: SocketAddr,
        pg: bool,
        h2: bool,
    ) -> io::Result<TlsStream<TcpStream>> {
        let mut socket = TcpStream::connect(address).await?;
        if pg {
            socket.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await?;
            if socket.read_u8().await? != b'S' {
                return Err(io::Error::other("SSLRequest rejected"));
            }
        }
        TlsConnector::from(f.client_config(&f.app, h2))
            .connect(ServerName::try_from("localhost").unwrap(), socket)
            .await
    }
    let endpoints = [
        (native.address, false, false),
        (pg.address, true, false),
        (f.http.address, false, false),
        (f.grpc.address, false, true),
    ];
    for (address, is_pg, h2) in endpoints {
        // Wait for the previous physical owner to be reclaimed, then retain one
        // authenticated socket; every other protocol must share the same cap.
        let held = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match connect(&f, address, is_pg, h2).await {
                    Ok(stream) => break stream,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .unwrap();
        for (contender, pg, h2) in endpoints {
            assert!(
                tokio::time::timeout(Duration::from_secs(2), connect(&f, contender, pg, h2))
                    .await
                    .unwrap()
                    .is_err()
            );
        }
        drop(held);
    }
    native.shutdown().await.unwrap();
    pg.shutdown().await.unwrap();
    f.stop().await;
}

fn row_count(result: &Value) -> usize {
    result["sql"]["rows"].as_array().map_or(0, Vec::len)
}

#[tokio::test]
async fn http_and_grpc_reject_wrong_ca_and_expired_bound_certificates() {
    use api_support::{TestCert, certificate};
    use rcgen::{CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, KeyPair};
    use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
    let f = Fixture::new().await;
    let wrong_ca = certificate(None, false);
    let wrong = certificate(Some(&wrong_ca), false);
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, "expired test client");
    params.distinguished_name = name;
    params.not_before = rcgen::date_time_ymd(2019, 1, 1);
    params.not_after = rcgen::date_time_ymd(2020, 1, 1);
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let expired = TestCert {
        cert: params.signed_by(&key, &f.ca.cert, &f.ca.key).unwrap(),
        key,
    };
    // Bind both fingerprints so a missing TLS check could not be hidden by an
    // unrelated application-level unbound-identity rejection.
    let mut bindings = f.bindings.clone();
    for certificate in [&wrong, &expired] {
        bindings.push(giggles_server::Binding {
            fingerprint: giggles_server::tls::fingerprint(certificate.cert.der()),
            role: f.bindings[0].role.clone(),
        });
    }
    f.service.reload_bindings(bindings).unwrap();
    for certificate in [&wrong, &expired] {
        for h2 in [false, true] {
            assert!(
                f.http(
                    certificate,
                    "POST",
                    "/v1/sessions",
                    None,
                    b"{}".to_vec(),
                    h2
                )
                .await
                .is_err()
            );
        }
        let endpoint = Channel::from_shared(format!("https://127.0.0.1:{}", f.grpc.address.port()))
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(Certificate::from_pem(f.ca.cert.pem()))
                    .identity(Identity::from_pem(
                        certificate.cert.pem(),
                        certificate.key.serialize_pem(),
                    )),
            )
            .unwrap();
        if let Ok(channel) = endpoint.connect().await {
            let mut client = proto::giggles_client::GigglesClient::new(channel);
            let error = client
                .open_session(proto::OpenSessionRequest::default())
                .await
                .unwrap_err();
            assert_ne!(
                error.code(),
                tonic::Code::Unauthenticated,
                "must fail TLS, before the bound fingerprint reaches application authentication"
            );
        }
    }
    let token = f.open_http(&f.app, false).await;
    assert_eq!(token.len(), 64);
    f.stop().await;
}





#[tokio::test]
async fn http1_and_http2_persistent_transactions_survive_reconnect() {
    for h2 in [false, true] {
        let f = Fixture::new().await;
        let token = f.open_http(&f.app, h2).await;
        assert_eq!(
            f.exec_http(&f.app, &token, json!({"sql":{"sql":"BEGIN"}}), h2)
                .await["transactionState"],
            "TRANSACTION_STATE_IN_TRANSACTION"
        );
        let insert=f.exec_http(&f.app,&token,json!({"sql":{"sql":"INSERT INTO app (id,body) VALUES ($1,$2)","params":[{"intValue":"9223372036854775807"},{"textValue":"snow 雪\u{0000}"}]}}),h2).await;
        assert_eq!(insert["code"], "ok");
        let outsider = f.open_http(&f.app, h2).await;
        let query = json!({"sql":{"sql":"SELECT id,body FROM app WHERE id=$1","params":[{"intValue":"9223372036854775807"}]}});
        assert_eq!(
            row_count(&f.exec_http(&f.app, &outsider, query.clone(), h2).await),
            0
        );
        let inside = f.exec_http(&f.app, &token, query.clone(), h2).await;
        assert_eq!(
            inside["sql"]["rows"][0]["values"][0]["intValue"],
            "9223372036854775807"
        );
        assert_eq!(
            inside["sql"]["rows"][0]["values"][1]["textValue"],
            "snow 雪\u{0000}"
        );
        assert_eq!(
            f.exec_http(&f.app, &token, json!({"sql":{"sql":"COMMIT"}}), h2)
                .await["transactionState"],
            "TRANSACTION_STATE_IDLE"
        );
        assert_eq!(
            row_count(&f.exec_http(&f.app, &outsider, query, h2).await),
            1
        );
        let (status, _) = f
            .http(&f.app, "DELETE", "/v1/session", Some(&token), vec![], h2)
            .await
            .unwrap();
        assert_eq!(status, 204);
        let (status, _) = f
            .http(
                &f.app,
                "POST",
                "/v1/session/execute",
                Some(&token),
                br#"{"ping":{}}"#.to_vec(),
                h2,
            )
            .await
            .unwrap();
        assert_eq!(status, 404);
        f.stop().await;
    }
}

#[tokio::test]
async fn http_open_grpc_begin_http_write_grpc_commit_and_rollback_recovery() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    let mut client = f.grpc_client(&f.app).await;
    assert_eq!(
        client
            .execute(rpc(&token, sql("BEGIN")))
            .await
            .unwrap()
            .into_inner()
            .code,
        "ok"
    );
    assert_eq!(
        f.exec_http(
            &f.app,
            &token,
            json!({"sql":{"sql":"INSERT INTO app (id,body) VALUES (1,'cross')"}}),
            true
        )
        .await["code"],
        "ok"
    );
    assert_eq!(
        client
            .execute(rpc(&token, sql("COMMIT")))
            .await
            .unwrap()
            .into_inner()
            .code,
        "ok"
    );
    let reply = client
        .execute(rpc(&token, sql("SELECT id,body FROM app WHERE id=1")))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        reply.sql.unwrap().rows[0].values[1].value,
        Some(proto::value::Value::TextValue("cross".into()))
    );
    assert_eq!(
        client
            .execute(rpc(&token, sql("BEGIN")))
            .await
            .unwrap()
            .into_inner()
            .code,
        "ok"
    );
    let duplicate = client
        .execute(rpc(&token, sql("INSERT INTO app (id) VALUES (1)")))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(duplicate.code, "23505");
    assert_eq!(
        duplicate.transaction_state,
        proto::TransactionState::Aborted as i32
    );
    assert_eq!(
        client
            .execute(rpc(&token, sql("SELECT id FROM app LIMIT 1")))
            .await
            .unwrap()
            .into_inner()
            .code,
        "25P02"
    );
    assert_eq!(
        client
            .execute(rpc(&token, sql("ROLLBACK")))
            .await
            .unwrap()
            .into_inner()
            .code,
        "ok"
    );
    assert_eq!(
        client
            .execute(rpc(&token, sql("SELECT * FROM other LIMIT 1")))
            .await
            .unwrap()
            .into_inner()
            .code,
        "42501"
    );
    client
        .close_session(rpc(&token, proto::CloseSessionRequest {}))
        .await
        .unwrap();
    drop(client);
    f.stop().await;
}

#[tokio::test]
async fn grpc_typed_values_and_close_discard_uncommitted_rows() {
    let f = Fixture::new().await;
    let mut client = f.grpc_client(&f.app).await;
    let token = client
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;
    use proto::value::Value as V;
    let values = vec![
        V::IntValue(i64::MIN),
        V::NullValue(0),
        V::BoolValue(false),
        V::BytesValue(vec![0, 255, 128]),
    ];
    let request = proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: "INSERT INTO app VALUES ($1,$2,$3,$4)".into(),
            params: values
                .clone()
                .into_iter()
                .map(|value| proto::Value { value: Some(value) })
                .collect(),
        })),
        budget_ms: None, operation_id: None,
    };
    assert_eq!(
        client
            .execute(rpc(&token, request))
            .await
            .unwrap()
            .into_inner()
            .code,
        "ok"
    );
    let result = client
        .execute(rpc(&token, sql("SELECT * FROM app LIMIT 1")))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        result.sql.unwrap().rows[0]
            .values
            .iter()
            .map(|v| v.value.clone().unwrap())
            .collect::<Vec<_>>(),
        values
    );
    client.execute(rpc(&token, sql("BEGIN"))).await.unwrap();
    client
        .execute(rpc(&token, sql("INSERT INTO app (id) VALUES (2)")))
        .await
        .unwrap();
    client
        .close_session(rpc(&token, proto::CloseSessionRequest {}))
        .await
        .unwrap();
    assert!(
        f.admin
            .sql_session()
            .execute("SELECT id FROM app WHERE id=2", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        client
            .execute(rpc(&token, sql("SELECT id FROM app LIMIT 1")))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    drop(client);
    f.stop().await;
}

#[tokio::test]
async fn identity_binding_and_remove_readd_fence_sessions() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    let (status, _) = f
        .http(
            &f.other,
            "POST",
            "/v1/session/execute",
            Some(&token),
            br#"{"ping":{}}"#.to_vec(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(status, 404);
    let mut client = f.grpc_client(&f.app).await;
    assert_eq!(
        client
            .execute(rpc(&token, sql("BEGIN")))
            .await
            .unwrap()
            .into_inner()
            .code,
        "ok"
    );
    client
        .execute(rpc(&token, sql("INSERT INTO app (id) VALUES (3)")))
        .await
        .unwrap();
    f.service.reload_bindings(vec![]).unwrap();
    f.runtime.invalidate_bindings();
    f.service.reload_bindings(f.bindings.clone()).unwrap();
    let err = client
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    let (status, _) = f
        .http(
            &f.app,
            "POST",
            "/v1/session/execute",
            Some(&token),
            br#"{"sql":{"sql":"COMMIT"}}"#.to_vec(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(status, 404);
    assert!(
        f.admin
            .sql_session()
            .execute("SELECT id FROM app WHERE id=3", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    let unbound = f
        .http(
            &f.unbound,
            "POST",
            "/v1/sessions",
            None,
            b"{}".to_vec(),
            false,
        )
        .await;
    assert!(unbound.is_err() || unbound.unwrap().0 == 401);
    drop(client);
    f.stop().await;
}

#[tokio::test]
async fn strict_http_validation_and_caps_never_dispatch() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    for body in [
        br#"{"sql":{"sql":"BEGIN"},"ping":{}}"#.to_vec(),
        br#"{"ping":{},"ping":{}}"#.to_vec(),
        br#"{"unknown":{}}"#.to_vec(),
        br#"{"status":{"operation":1}}"#.to_vec(),
        br#"{"sql":{"sql":"INSERT INTO app (id) VALUES ($1)","params":[{"intValue":"01"}]}}"#
            .to_vec(),
    ] {
        let (status, _) = f
            .http(
                &f.app,
                "POST",
                "/v1/session/execute",
                Some(&token),
                body,
                false,
            )
            .await
            .unwrap();
        assert_eq!(status, 400);
    }
    let (status, _) = f
        .http(
            &f.app,
            "POST",
            "/v1/session/execute",
            Some(&token),
            vec![b' '; giggles_server::MAX_FRAME + 1],
            true,
        )
        .await
        .unwrap();
    assert_eq!(status, 413);
    for (method, path, expected) in [
        ("GET", "/v1/sessions", 405),
        ("POST", "/nope", 404),
        ("POST", "/v1/sessions?x=1", 400),
    ] {
        let (status, _) = f
            .http(&f.app, method, path, None, b"{}".to_vec(), false)
            .await
            .unwrap();
        assert_eq!(status, expected);
    }
    let (status, _) = f
        .http_headers(
            &f.app,
            "POST",
            "/v1/session/execute",
            Some(&token),
            br#"{"ping":{}}"#.to_vec(),
            false,
            &[("content-encoding", "gzip")],
        )
        .await
        .unwrap();
    assert_eq!(status, 415);
    let (status, _) = f
        .http_headers(
            &f.app,
            "POST",
            "/v1/session/execute",
            Some(&token),
            br#"{"ping":{}}"#.to_vec(),
            false,
            &[("x-giggles-session", &token)],
        )
        .await
        .unwrap();
    assert_eq!(status, 400);
    assert!(
        f.admin
            .sql_session()
            .execute("SELECT id FROM app LIMIT 10", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        f.exec_http(&f.app, &token, json!({"ping":{}}), false).await["code"],
        "ok"
    );
    f.stop().await;
}

#[tokio::test]
async fn shared_logical_session_capacity_and_zero_deadline() {
    let f = Fixture::with_limits(ApiLimits { sessions: 1 }).await;
    let token = f.open_http(&f.app, false).await;
    let mut client = f.grpc_client(&f.app).await;
    assert_eq!(
        client
            .open_session(proto::OpenSessionRequest::default())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    let mut write = sql("INSERT INTO app (id) VALUES (1)");
    write.budget_ms = Some(0);
    let result = client.execute(rpc(&token, write)).await;
    match result {
        Ok(reply) => assert_eq!(reply.into_inner().code, "57014"),
        Err(error) => assert_eq!(error.code(), tonic::Code::DeadlineExceeded),
    }
    assert!(
        f.admin
            .sql_session()
            .execute("SELECT id FROM app LIMIT 1", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    client
        .close_session(rpc(&token, proto::CloseSessionRequest {}))
        .await
        .unwrap();
    let next = client
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;
    assert_ne!(token, next);
    drop(client);
    f.stop().await;
}





#[tokio::test]
async fn dd68_f9_native_surface_keeps_authority_for_deferred_routines() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    let mut client = f.grpc_client(&f.app).await;
    for query in ["SELECT nosuchfn()", "SELECT nosuchschema.lower('a')"] {
        let reply = client
            .execute(rpc(&token, sql(query)))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.code, "42501", "{query}: {reply:?}");
    }
    drop(client);
    f.stop().await;
}
