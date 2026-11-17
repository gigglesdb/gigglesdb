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
async fn native_http_and_grpc_task_disclosure_debits_match_exactly() {
    use giggles_core::{TaskId, TaskResources};
    let f = Fixture::new().await;
    let token = f.open_http(&f.task, false).await;
    let wire = json!({"preview":{"request":{"operation":"702","selection":{"keys":{"items":[{"tenant":"42","row":"0"}]}},"column":"COLUMN_STATUS","after":"DELIVERY_STATUS_QUEUED"}}});
    let request =
        giggles_server::api::json::decode_execute(&serde_json::to_vec(&wire).unwrap()).unwrap();
    let giggles_server::api::codec::Invocation::Legacy(native) =
        giggles_server::api::codec::decode_execute(request.clone())
            .unwrap()
            .request
    else {
        panic!("legacy task request expected")
    };
    let used = || f.admin.task_resources(TaskId(1)).unwrap().unwrap().used;
    let before = used();
    let reply = f
        .service
        .session(&giggles_server::tls::fingerprint(f.task.cert.der()))
        .unwrap()
        .handle(native);
    assert_eq!(reply.code, "preview");
    let after_native = used();
    let http = f.exec_http(&f.task, &token, wire, false).await;
    let after_http = used();
    let mut grpc = f.grpc_client(&f.task).await;
    let grpc_reply = grpc
        .execute(rpc(&token, request))
        .await
        .unwrap()
        .into_inner();
    let after_grpc = used();
    fn delta(a: TaskResources, b: TaskResources) -> TaskResources {
        TaskResources {
            read_rows: b.read_rows - a.read_rows,
            disclosed_rows: b.disclosed_rows - a.disclosed_rows,
            encoded_bytes: b.encoded_bytes - a.encoded_bytes,
        }
    }
    let debit = delta(before, after_native);
    assert!(debit.read_rows > 0 && debit.disclosed_rows > 0 && debit.encoded_bytes > 0);
    assert_eq!(delta(after_native, after_http), debit);
    assert_eq!(delta(after_http, after_grpc), debit);
    assert_eq!(http["digest"].as_str(), reply.digest.as_deref());
    assert_eq!(grpc_reply.digest, reply.digest);
    drop(grpc);
    f.stop().await;
}

#[tokio::test]
async fn exhausted_task_disclosure_never_echoes_operation_count_or_digest() {
    use giggles_core::{TaskId, TaskResources};
    let f = Fixture::with_task_resources(TaskResources {
        read_rows: 1000,
        disclosed_rows: 1000,
        encoded_bytes: 0,
    })
    .await;
    let token = f.open_http(&f.task, true).await;
    let request = json!({"preview":{"request":{"operation":"703","selection":{"keys":{"items":[{"tenant":"42","row":"0"}]}},"column":"COLUMN_STATUS","after":"DELIVERY_STATUS_QUEUED"}}});
    let http = f.exec_http(&f.task, &token, request.clone(), true).await;
    assert_ne!(http["code"], "preview");
    for field in ["operation", "count", "digest", "sql"] {
        assert!(http.get(field).is_none(), "{field}");
    }
    let mut grpc = f.grpc_client(&f.task).await;
    let request =
        giggles_server::api::json::decode_execute(&serde_json::to_vec(&request).unwrap()).unwrap();
    let reply = grpc
        .execute(rpc(&token, request))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(reply.code, "preview");
    assert_eq!(reply.operation, None);
    assert_eq!(reply.count, None);
    assert_eq!(reply.digest, None);
    assert_eq!(reply.sql, None);
    assert_eq!(
        f.admin
            .task_resources(TaskId(1))
            .unwrap()
            .unwrap()
            .used
            .encoded_bytes,
        0
    );
    drop(grpc);
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
        budget_ms: None,
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
async fn task_preview_execute_status_and_role_redaction_cross_protocol() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.task, false).await;
    let task = json!({"operation":"701","selection":{"keys":{"items":[{"tenant":"42","row":"0"}]}},"column":"COLUMN_STATUS","after":"DELIVERY_STATUS_QUEUED"});
    let preview = f
        .exec_http(
            &f.task,
            &token,
            json!({"preview":{"request":task.clone()}}),
            false,
        )
        .await;
    assert_eq!(preview["code"], "preview");
    assert_eq!(preview["count"], "1");
    assert!(preview.get("effects").is_none());
    let executed = f
        .exec_http(&f.task, &token, json!({"execute":{"request":task}}), true)
        .await;
    assert_eq!(executed["code"], "committed");
    let mut client = f.grpc_client(&f.task).await;
    let status = proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Status(
            proto::StatusRequest {
                operation: Some(701),
            },
        )),
        budget_ms: None,
    };
    let reply = client
        .execute(rpc(&token, status))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.code, "committed");
    assert_eq!(reply.count, Some(1));
    assert_eq!(reply.digest.as_deref(), executed["digest"].as_str());
    assert_ne!(
        client
            .execute(rpc(&token, sql("BEGIN")))
            .await
            .unwrap()
            .into_inner()
            .code,
        "ok"
    );
    drop(client);
    f.stop().await;
}

#[tokio::test]
async fn task_approval_retry_conflict_and_repair_sql_use_existing_authority() {
    use giggles_core::{Key, Principal, Row, RowId, Status, TaskId, TenantId, Time};
    let f = Fixture::new().await;
    for row in 4..14 {
        f.admin
            .put(
                Key {
                    tenant: TenantId(42),
                    row: RowId(row),
                },
                Row {
                    status: Status::Failed,
                    failed_at: 103,
                    attempt_count: 0,
                    version: 0,
                },
            )
            .unwrap();
    }
    let token = f.open_http(&f.task, false).await;
    let task = json!({"operation":"800","selection":{"keys":{"items":(0..12).map(|row|json!({"tenant":"42","row":row.to_string()})).collect::<Vec<_>>()}},"column":"COLUMN_STATUS","after":"DELIVERY_STATUS_QUEUED"});
    let preview = f
        .exec_http(
            &f.task,
            &token,
            json!({"preview":{"request":task.clone()}}),
            false,
        )
        .await;
    assert_eq!(preview["count"], "12");
    let wire = giggles_server::api::json::decode_execute(
        &serde_json::to_vec(&json!({"execute":{"request":task.clone()}})).unwrap(),
    )
    .unwrap();
    let giggles_server::api::codec::Invocation::Legacy(giggles_server::Request::Execute {
        request: native,
        ..
    }) = giggles_server::api::codec::decode_execute(wire.clone())
        .unwrap()
        .request
    else {
        panic!("task execute");
    };
    let mut client = f.grpc_client(&f.task).await;
    // Denied decisions are durable under their operation IDs too. Approving a
    // later attempt cannot change the original decision on retry.
    let mut without_approval = wire.clone();
    if let Some(proto::execute_request::Request::Execute(execute)) = &mut without_approval.request {
        execute.request.as_mut().unwrap().operation = Some(799);
    }
    let denied = client
        .execute(rpc(&token, without_approval))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(denied.code, "GD001");
    let approved = f
        .admin
        .executor(TaskId(1), Principal(2))
        .unwrap()
        .preview(&native, Time::Certified(api_support::now()))
        .unwrap();
    let approval = f
        .admin
        .approve(Principal(3), approved, Time::Certified(api_support::now()))
        .unwrap();
    let mut wire = wire;
    let Some(proto::execute_request::Request::Execute(ref mut execute)) = wire.request else {
        panic!("task execute");
    };
    execute.approval = Some(approval.0);
    let committed = client
        .execute(rpc(&token, wire))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(committed.code, "committed");
    assert_eq!(committed.count, Some(12));
    let retry = f
        .exec_http(
            &f.task,
            &token,
            json!({"execute":{"request":task.clone(),"approval":approval.0.to_string()}}),
            true,
        )
        .await;
    assert_eq!(retry["code"], "committed");
    assert_eq!(retry["digest"].as_str(), committed.digest.as_deref());
    let mut changed = task;
    changed["selection"]["keys"]["items"] = json!([{"tenant":"42","row":"0"}]);
    let conflict = f
        .exec_http(
            &f.task,
            &token,
            json!({"execute":{"request":changed,"approval":approval.0.to_string()}}),
            false,
        )
        .await;
    assert_eq!(conflict["code"], "GD002");
    let repaired=f.exec_http(&f.task,&token,json!({"repairSql":{"operation":"801","sql":"UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 12"}}),false).await;
    assert_eq!(repaired["code"], "committed");
    assert_eq!(repaired["count"], "1");
    let repaired = client
        .execute(rpc(
            &token,
            proto::ExecuteRequest {
                request: Some(proto::execute_request::Request::RepairSql(
                    proto::RepairSqlRequest {
                        operation: Some(802),
                        sql:
                            "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 13"
                                .into(),
                        approval: None,
                    },
                )),
                budget_ms: None,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(repaired.code, "committed");
    assert_eq!(repaired.count, Some(1));
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
