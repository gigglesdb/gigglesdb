//! Role listener network and retained authority regressions.
mod api_support;
use api_support::{Fixture, rpc};
mod api_role_support;
use api_role_support::{FIRST, http, proof};
use giggles_server::{
    api::{ApiLimits, ApiRuntime, net::serve_role_bound, proto},
    auth::RoleApiConfig,
};
use serde_json::{Value, json};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
};

async fn start(f: &Fixture, trust: bool, grpc: bool) -> (giggles_server::Running, Arc<ApiRuntime>) {
    let config: RoleApiConfig = serde_json::from_value(json!({"http":"127.0.0.1:0","tls":"disabled","loopback_trust":trust,"idle_timeout_seconds":10,"max_age_seconds":10})).unwrap();
    let settings = Arc::new(config.build().unwrap());
    let runtime =
        ApiRuntime::new_role(f.service.clone(), ApiLimits::default(), settings.clone()).unwrap();
    let (_, gate) = watch::channel(true);
    let listener = serve_role_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        runtime.clone(),
        settings,
        gate,
        grpc,
    )
    .await
    .unwrap();
    (listener, runtime)
}
fn roles(f: &Fixture) {
    f.admin
        .roles()
        .create(giggles_core::roles::NewRole {
            name: "role_admin".into(),
            login: true,
            superuser: true,
            ..Default::default()
        })
        .unwrap();
    for (name, login) in [("r", true), ("nologin", false)] {
        f.admin
            .roles()
            .create(giggles_core::roles::NewRole {
                name: name.into(),
                login,
                password: Some(giggles_server::auth::scram::Verifier::generate("secret")),
                ..Default::default()
            })
            .unwrap();
    }
}
#[tokio::test]
async fn native_sql_pg_typeof_evaluates_its_argument() {
    let f = Fixture::catalog_empty().await;
    roles(&f);
    let (listener, runtime) = start(&f, true, false).await;
    let addr = listener.address;
    let (status, session) = http(
        addr,
        "POST",
        "/v1/sessions",
        None,
        json!({"role":"role_admin","database":"postgres"}),
    )
    .await;
    assert_eq!(status, 201, "{session}");
    let token = session["sessionId"].as_str().unwrap();

    for (sql, params, expected) in [
        ("SELECT pg_typeof(1/0)", vec![], Err("22012")),
        ("SELECT pg_typeof('today'::text::date)", vec![], Ok("date")),
        (
            "SELECT pg_typeof(10 / $1)",
            vec![json!({"intValue":"2"})],
            Ok("integer"),
        ),
        (
            "SELECT pg_typeof(10 / $1)",
            vec![json!({"intValue":"0"})],
            Err("22012"),
        ),
    ] {
        let (status, reply) = http(
            addr,
            "POST",
            "/v1/session/execute",
            Some(token),
            json!({"sql":{"sql":sql,"params":params}}),
        )
        .await;
        assert_eq!(status, 200, "{sql}: {reply}");
        match expected {
            Ok(value) => {
                assert_eq!(reply["code"], "ok", "{sql}: {reply}");
                assert_eq!(
                    reply["sql"]["rows"][0]["values"],
                    json!([{"textValue":value}])
                );
            }
            Err(state) => assert_eq!(reply["code"], state, "{sql}: {reply}"),
        }
    }

    listener.shutdown().await.unwrap();
    runtime.shutdown().await;
    f.stop().await;
}

#[tokio::test]
async fn http_role_signin_reconnect_revocation_and_expiry() {
    let f = Fixture::catalog_empty().await;
    roles(&f);
    let pg_config: giggles_server::auth::PostgresqlConfig = serde_json::from_value(json!({
        "listen":"127.0.0.1:0", "tls":"disabled", "loopback_trust":true
    }))
    .unwrap();
    let pg = giggles_server::auth::serve_postgresql(
        "127.0.0.1:0".parse().unwrap(),
        f.service.clone(),
        Arc::new(pg_config.build().unwrap()),
    )
    .await
    .unwrap();
    let (admin, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(pg.address.port())
        .user("role_admin")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let pg_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    let (listener, runtime) = start(&f, true, false).await;
    let addr = listener.address;
    for body in [json!({}), json!({"task":"t"})] {
        assert_eq!(http(addr, "POST", "/v1/sessions", None, body).await.0, 400);
    }
    for body in [
        json!({"role":"missing","database":"postgres"}),
        json!({"role":"nologin","database":"postgres"}),
        json!({"role":"r","database":"forbidden"}),
    ] {
        assert_eq!(http(addr, "POST", "/v1/sessions", None, body).await.0, 401);
    }
    let open = || json!({"role":"r","database":"postgres"});
    let (status, reply) = http(addr, "POST", "/v1/sessions", None, open()).await;
    assert_eq!(status, 201, "{reply}");
    assert_eq!(reply["idleTimeoutSeconds"], 10);
    assert_eq!(reply["maxAgeSeconds"], 10);
    let token = reply["sessionId"].as_str().unwrap();
    for body in [json!({"sql":{"sql":"SELECT 1"}}), json!({"ping":{}})] {
        let (status, reply) = http(addr, "POST", "/v1/session/execute", Some(token), body).await;
        assert_eq!(status, 200, "{reply}");
        assert_eq!(reply["code"], "ok", "{reply}");
    }
    admin
        .batch_execute("ALTER ROLE r PASSWORD 'x'")
        .await
        .unwrap();
    assert_eq!(
        http(
            addr,
            "POST",
            "/v1/session/execute",
            Some(token),
            json!({"ping":{}})
        )
        .await
        .0,
        200
    );
    admin.batch_execute("ALTER ROLE r NOLOGIN").await.unwrap();
    assert_eq!(
        http(
            addr,
            "POST",
            "/v1/session/execute",
            Some(token),
            json!({"ping":{}})
        )
        .await
        .0,
        401
    );
    admin.batch_execute("ALTER ROLE r LOGIN").await.unwrap();
    let (_, reply) = http(addr, "POST", "/v1/sessions", None, open()).await;
    assert_eq!(
        http(
            addr,
            "DELETE",
            "/v1/session",
            Some(reply["sessionId"].as_str().unwrap()),
            Value::Null
        )
        .await
        .0,
        204
    );
    let (_, reply) = http(addr, "POST", "/v1/sessions", None, open()).await;
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert_eq!(
        http(
            addr,
            "POST",
            "/v1/session/execute",
            Some(reply["sessionId"].as_str().unwrap()),
            json!({"ping":{}})
        )
        .await
        .0,
        401
    );
    listener.shutdown().await.unwrap();
    runtime.shutdown().await;
    let (listener, runtime) = start(&f, false, false).await;
    assert_eq!(
        http(listener.address, "POST", "/v1/sessions", None, open())
            .await
            .0,
        401
    );
    listener.shutdown().await.unwrap();
    runtime.shutdown().await;
    drop(admin);
    pg_task.abort();
    let _ = pg_task.await;
    pg.shutdown().await.unwrap();
    f.stop().await;
}
#[tokio::test]
async fn grpc_h2c_role_signin_and_reconnect() {
    let f = Fixture::catalog_empty().await;
    roles(&f);
    let (listener, runtime) = start(&f, true, true).await;
    let endpoint = format!("http://{}", listener.address);
    let mut client = proto::giggles_client::GigglesClient::connect(endpoint.clone())
        .await
        .unwrap();
    let reply = client
        .open_session(proto::OpenSessionRequest {
            role: Some("r".into()),
            database: Some("postgres".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    drop(client);
    let mut client = proto::giggles_client::GigglesClient::connect(endpoint)
        .await
        .unwrap();
    for body in [json!({"sql":{"sql":"SELECT 1"}}), json!({"ping":{}})] {
        let request =
            giggles_server::api::json::decode_execute(&serde_json::to_vec(&body).unwrap()).unwrap();
        client
            .execute(rpc(&reply.session_id, request))
            .await
            .unwrap();
    }
    client
        .close_session(rpc(&reply.session_id, proto::CloseSessionRequest {}))
        .await
        .unwrap();
    listener.shutdown().await.unwrap();
    runtime.shutdown().await;
    f.stop().await;
}

#[tokio::test]
async fn mapped_certificate_role_signin() {
    let mut f = Fixture::catalog_empty().await;
    roles(&f);
    // The ordinary listener continues rejecting role fields before replacement.
    assert_eq!(
        f.http(
            &f.app,
            "POST",
            "/v1/sessions",
            None,
            serde_json::to_vec(&json!({"role":"r","database":"postgres"})).unwrap(),
            false
        )
        .await
        .unwrap()
        .0,
        400
    );
    let token = f.open_http(&f.app, false).await;
    assert_eq!(
        f.exec_http(&f.app, &token, json!({"ping":{}}), false).await["code"],
        "ok"
    );
    let config: RoleApiConfig = serde_json::from_value(json!({
        "http":"127.0.0.1:0","tls":"required",
        "certificate":f.path.join("server.pem"),"private_key":f.path.join("server.key.TEST-ONLY"),
        "client_ca":f.path.join("ca.pem"),
        "certificates":{(giggles_server::tls::fingerprint(f.app.cert.der().as_ref())):"r"}
    }))
    .unwrap();
    let settings = Arc::new(config.build().unwrap());
    let runtime =
        ApiRuntime::new_role(f.service.clone(), ApiLimits::default(), settings.clone()).unwrap();
    let (_, gate) = watch::channel(true);
    let listener = serve_role_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        runtime.clone(),
        settings,
        gate,
        false,
    )
    .await
    .unwrap();
    let old = std::mem::replace(&mut f.http, listener);
    old.shutdown().await.unwrap();
    // A mapped certificate authenticates only its mapped role, never another LOGIN role.
    let (status, _) = f
        .http(
            &f.app,
            "POST",
            "/v1/sessions",
            None,
            serde_json::to_vec(&json!({"role":"role_admin","database":"postgres"})).unwrap(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(status, 401);
    for h2 in [false, true] {
        let (status, bytes) = f
            .http(
                &f.app,
                "POST",
                "/v1/sessions",
                None,
                serde_json::to_vec(&json!({"role":"r","database":"postgres"})).unwrap(),
                h2,
            )
            .await
            .unwrap();
        assert_eq!(status, 201, "{}", String::from_utf8_lossy(&bytes));
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            f.exec_http(
                &f.app,
                value["sessionId"].as_str().unwrap(),
                json!({"ping":{}}),
                h2
            )
            .await["code"],
            "ok"
        );
    }
    f.stop().await;
    runtime.shutdown().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn task_only_role_is_refused() {
    let f = Fixture::catalog_empty_engine(giggles_core::Engine::V2).await;
    roles(&f);
    f.admin.roles().set_task_only("r", true).unwrap();
    let (listener, runtime) = start(&f, true, false).await;
    assert_eq!(
        http(
            listener.address,
            "POST",
            "/v1/sessions",
            None,
            json!({"role":"r","database":"postgres"})
        )
        .await
        .0,
        401
    );
    listener.shutdown().await.unwrap();
    runtime.shutdown().await;
    let (listener, runtime) = start(&f, false, false).await;
    let challenge = scram_start(listener.address, "r").await;
    assert_eq!(
        scram_finish(listener.address, &challenge, "secret").await.0,
        401
    );
    listener.shutdown().await.unwrap();
    runtime.shutdown().await;
    f.stop().await;
}

async fn scram_start(addr: SocketAddr, role: &str) -> Value {
    let (status, reply) = http(
        addr,
        "POST",
        "/v1/sessions",
        None,
        json!({"role":role,"database":"postgres","scram":{"clientFirst":FIRST}}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(reply["sessionId"], "");
    reply["scram"].clone()
}
async fn scram_finish(addr: SocketAddr, challenge: &Value, password: &str) -> (u16, Value) {
    let (final_msg, expected) = proof(password, challenge["serverFirst"].as_str().unwrap());
    let result = http(
        addr,
        "POST",
        "/v1/sessions",
        None,
        json!({"scram":{"exchange":challenge["exchange"],"clientFinal":final_msg}}),
    )
    .await;
    if result.0 == 201 {
        assert_eq!(result.1["scramServerFinal"], expected);
    }
    result
}
#[tokio::test]
async fn scram_http_proof_single_use_expiry_and_capacity() {
    let f = Fixture::catalog_empty().await;
    roles(&f);
    let (listener, runtime) = start(&f, false, false).await;
    let addr = listener.address;
    assert_eq!(
        http(
            addr,
            "POST",
            "/v1/sessions",
            None,
            json!({"role":"r","database":"postgres","password":"secret"})
        )
        .await
        .0,
        401
    );
    // Each helper opens a new TCP connection, including continuation.
    let challenge = scram_start(addr, "r").await;
    let (status, reply) = scram_finish(addr, &challenge, "secret").await;
    assert_eq!(status, 201);
    assert_eq!(
        http(
            addr,
            "POST",
            "/v1/session/execute",
            reply["sessionId"].as_str(),
            json!({"sql":{"sql":"SELECT 1"}})
        )
        .await
        .0,
        200
    );
    for role in ["r", "missing", "nologin"] {
        let challenge = scram_start(addr, role).await;
        assert_eq!(
            scram_finish(
                addr,
                &challenge,
                if role == "r" { "wrong" } else { "secret" }
            )
            .await
            .0,
            401
        );
        assert_eq!(scram_finish(addr, &challenge, "secret").await.0, 401);
    }
    let challenge = scram_start(addr, "r").await;
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert_eq!(scram_finish(addr, &challenge, "secret").await.0, 401);
    for _ in 0..64 {
        scram_start(addr, "r").await;
    }
    assert_eq!(
        http(
            addr,
            "POST",
            "/v1/sessions",
            None,
            json!({"role":"r","database":"postgres","scram":{"clientFirst":FIRST}})
        )
        .await
        .0,
        429
    );
    listener.shutdown().await.unwrap();
    runtime.shutdown().await;
    let (listener, runtime) = start(&f, true, false).await;
    assert_eq!(
        http(
            listener.address,
            "POST",
            "/v1/sessions",
            None,
            json!({"role":"r","database":"postgres","scram":{"clientFirst":FIRST}})
        )
        .await
        .0,
        400
    );
    listener.shutdown().await.unwrap();
    runtime.shutdown().await;
    f.stop().await;
}
#[tokio::test]
async fn scram_grpc_h2c_reconnect() {
    let f = Fixture::catalog_empty().await;
    roles(&f);
    let (listener, runtime) = start(&f, false, true).await;
    let endpoint = format!("http://{}", listener.address);
    let mut client = proto::giggles_client::GigglesClient::connect(endpoint.clone())
        .await
        .unwrap();
    let reply = client
        .open_session(proto::OpenSessionRequest {
            role: Some("r".into()),
            database: Some("postgres".into()),
            credential: Some(proto::open_session_request::Credential::ScramStart(
                proto::ScramStart {
                    client_first: FIRST.into(),
                },
            )),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let challenge = reply.scram_challenge.unwrap();
    let (client_final, expected) = proof("secret", &challenge.server_first);
    drop(client);
    let mut client = proto::giggles_client::GigglesClient::connect(endpoint)
        .await
        .unwrap();
    let reply = client
        .open_session(proto::OpenSessionRequest {
            credential: Some(proto::open_session_request::Credential::ScramContinue(
                proto::ScramContinue {
                    exchange: challenge.exchange,
                    client_final,
                },
            )),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.scram_server_final.as_deref(), Some(expected.as_str()));
    client
        .execute(rpc(
            &reply.session_id,
            giggles_server::api::json::decode_execute(br#"{"sql":{"sql":"SELECT 1"}}"#).unwrap(),
        ))
        .await
        .unwrap();
    listener.shutdown().await.unwrap();
    runtime.shutdown().await;
    f.stop().await;
}
#[tokio::test]
async fn tls_password_http_versions_workload_rotation_and_listener_isolation() {
    let mut f = Fixture::catalog_empty().await;
    roles(&f);
    let config: RoleApiConfig = serde_json::from_value(json!({
        "http":"127.0.0.1:0","tls":"required",
        "certificate":f.path.join("server.pem"),"private_key":f.path.join("server.key.TEST-ONLY")
    }))
    .unwrap();
    let settings = Arc::new(config.build().unwrap());
    let runtime =
        ApiRuntime::new_role(f.service.clone(), ApiLimits::default(), settings.clone()).unwrap();
    let (_, gate) = watch::channel(true);
    let listener = serve_role_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        runtime.clone(),
        settings,
        gate,
        false,
    )
    .await
    .unwrap();
    // Keep both listener families alive in this process, using the same service.
    let top = std::mem::replace(&mut f.http, listener);
    let mut top = top;
    // ALPN must actually negotiate h2, rather than merely accepting h2 frames.
    let socket = TcpStream::connect(f.http.address).await.unwrap();
    let stream = tokio_rustls::TlsConnector::from(f.client_config(&f.app, true))
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            socket,
        )
        .await
        .unwrap();
    assert_eq!(stream.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
    drop(stream);
    for h2 in [true, false] {
        for (role, password, expected) in [
            ("r", "secret", 201),
            ("r", "wrong", 401),
            ("missing", "secret", 401),
        ] {
            let (status, bytes) = f
                .http(
                    &f.app,
                    "POST",
                    "/v1/sessions",
                    None,
                    serde_json::to_vec(
                        &json!({"role":role,"database":"postgres","password":password}),
                    )
                    .unwrap(),
                    h2,
                )
                .await
                .unwrap();
            assert_eq!(status, expected, "{}", String::from_utf8_lossy(&bytes));
            if status == 201 {
                let reply: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(
                    f.exec_http(
                        &f.app,
                        reply["sessionId"].as_str().unwrap(),
                        json!({"sql":{"sql":"SELECT 1"}}),
                        h2
                    )
                    .await["code"],
                    "ok"
                );
            }
        }
    }
    let (_, bytes) = f
        .http(
            &f.app,
            "POST",
            "/v1/sessions",
            None,
            serde_json::to_vec(&json!({"role":"r","database":"postgres","password":"secret"}))
                .unwrap(),
            false,
        )
        .await
        .unwrap();
    let reply: Value = serde_json::from_slice(&bytes).unwrap();
    let token = reply["sessionId"].as_str().unwrap();
    // Setup alone uses Admin; every workload read/write uses a role HTTP session.
    f.admin
        .sql_session()
        .execute("CREATE TABLE role_work (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    f.admin
        .roles()
        .grant_table(
            "role_work",
            "r",
            &[
                giggles_core::roles::TablePrivilege::Select,
                giggles_core::roles::TablePrivilege::Insert,
            ],
        )
        .unwrap();
    for request in [
        json!({"begin":{}}),
        json!({"operation":{"request":{"insert":{"table":"role_work","values":[{"column":"id","value":{"literal":{"intValue":"1"}}}]}}}}),
        json!({"operation":{"request":{"insert":{"table":"role_work","values":[{"column":"id","value":{"literal":{"intValue":"2"}}}]}}}}),
        json!({"commit":{}}),
    ] {
        let reply = f.exec_http(&f.app, token, request, false).await;
        assert_eq!(reply["code"], "ok", "{reply}");
    }
    // The fixture opens a fresh TCP/TLS connection for every call.
    let reply = f
        .exec_http(
            &f.app,
            token,
            json!({"sql":{"sql":"SELECT count(*) FROM role_work"}}),
            false,
        )
        .await;
    assert_eq!(reply["code"], "ok", "{reply}");
    assert_eq!(
        reply["sql"]["rows"][0]["values"][0],
        json!({"intValue":"2"}),
        "{reply}"
    );
    f.admin
        .roles()
        .create(giggles_core::roles::NewRole {
            name: "ungranted".into(),
            login: true,
            password: Some(giggles_server::auth::scram::Verifier::generate("secret")),
            ..Default::default()
        })
        .unwrap();
    let (status, bytes) = f
        .http(
            &f.app,
            "POST",
            "/v1/sessions",
            None,
            serde_json::to_vec(
                &json!({"role":"ungranted","database":"postgres","password":"secret"}),
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(status, 201);
    let denied: Value = serde_json::from_slice(&bytes).unwrap();
    let denied = f
        .exec_http(
            &f.app,
            denied["sessionId"].as_str().unwrap(),
            json!({"sql":{"sql":"SELECT count(*) FROM role_work"}}),
            false,
        )
        .await;
    assert_eq!(denied["code"], "42501", "{denied}");

    // Use a separate administrative pgwire path for ALTER ROLE, as in A4.
    let pg_config: giggles_server::auth::PostgresqlConfig = serde_json::from_value(json!({
        "listen":"127.0.0.1:0", "tls":"disabled", "loopback_trust":true
    }))
    .unwrap();
    let pg = giggles_server::auth::serve_postgresql(
        "127.0.0.1:0".parse().unwrap(),
        f.service.clone(),
        Arc::new(pg_config.build().unwrap()),
    )
    .await
    .unwrap();
    let (admin, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(pg.address.port())
        .user("role_admin")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let pg_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    admin
        .batch_execute("ALTER ROLE r PASSWORD 'new'")
        .await
        .unwrap();
    drop(admin);
    pg_task.abort();
    let _ = pg_task.await;
    pg.shutdown().await.unwrap();
    assert_eq!(
        f.exec_http(&f.app, token, json!({"ping":{}}), false).await["code"],
        "ok"
    );
    for (password, expected) in [("secret", 401), ("new", 201)] {
        let (status, _) = f
            .http(
                &f.app,
                "POST",
                "/v1/sessions",
                None,
                serde_json::to_vec(&json!({"role":"r","database":"postgres","password":password}))
                    .unwrap(),
                false,
            )
            .await
            .unwrap();
        assert_eq!(status, expected);
    }
    // Switch the fixture's destination without shutting down either listener.
    std::mem::swap(&mut f.http, &mut top);
    let bound_token = f.open_http(&f.app, false).await;
    let (status, _) = f
        .http(
            &f.app,
            "POST",
            "/v1/sessions",
            None,
            serde_json::to_vec(&json!({"role":"r","database":"postgres","password":"new"}))
                .unwrap(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(status, 400);
    let (status, bytes) = f
        .http(
            &f.app,
            "POST",
            "/v1/session/execute",
            Some(token),
            serde_json::to_vec(&json!({"ping":{}})).unwrap(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(status, 404);
    let error: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(error["error"]["code"], "SESSION_NOT_FOUND");
    std::mem::swap(&mut f.http, &mut top);
    let (status, _) = f
        .http(
            &f.app,
            "POST",
            "/v1/session/execute",
            Some(&bound_token),
            serde_json::to_vec(&json!({"ping":{}})).unwrap(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(status, 401);
    top.shutdown().await.unwrap();
    f.stop().await;
    runtime.shutdown().await;
}
