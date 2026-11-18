//! R3-1004: attempts to cross the ordinary/agent listener and task-session fences.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::parity::{self, *};
use agent_support::{Msg, Startup, TestCert, binding, certificate, error_of, raw_startup};
use bytes::Bytes;
use giggles_server::{
    Binding, Request, Role, Running,
    api::{json as wire_json, proto},
    auth::{PostgresListener, TlsMode, serve_postgresql},
    transport::Client,
};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use serde_json::{Value, json};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_rustls::TlsConnector;
use tonic::Code;

// Keep authentication independent of the tested SQL: credentials are issued by
// the controller on the ordinary listener, never manufactured in core.
async fn issue(f: &ParityFixture, task: &str) -> String {
    let issuer = f.password(CONTROLLER, CONTROLLER_PW).await.unwrap();
    let rows = issuer
        .simple_query(&format!("ISSUE CREDENTIAL FOR TASK \"{task}\""))
        .await
        .unwrap();
    rows.iter()
        .find_map(|row| match row {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(2).map(str::to_owned),
            _ => None,
        })
        .expect("ISSUE returns a secret")
}

async fn password_tls(
    f: &ParityFixture,
    address: SocketAddr,
    secret: &str,
    task: Option<&str>,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let mut config = tokio_postgres::Config::new();
    config
        .host("localhost")
        .hostaddr(address.ip())
        .port(address.port())
        .user(EXECUTOR)
        .password(secret)
        .dbname("giggles")
        .ssl_mode(SslMode::Require)
        .connect_timeout(Duration::from_secs(5));
    if let Some(task) = task {
        config.options(format!("-c giggles.task={task}"));
    }
    // The fixture's TLS requires a CA-valid client cert. This ordinary cert
    // carries no agent identity, so the agent must use the supplied password.
    let (client, connection) = config.connect(MakeRustlsConnect::new(f.app.client(&f.ca))).await?;
    tokio::spawn(connection);
    Ok(client)
}

fn bearer<T>(secret: &str, token: Option<&str>, body: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(body);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {secret}").parse().unwrap());
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("x-giggles-session", token.parse().unwrap());
    }
    request
}

// Only the bearer header is additional to parity::http_call. Keeping this
// helper local avoids changing the shared parity fixture for a negative test.
async fn http_bearer(
    f: &ParityFixture,
    address: SocketAddr,
    secret: &str,
    token: Option<&str>,
    body: Value,
) -> (u16, Value) {
    http_bearer_as(f, address, secret, token, body, &f.app).await
}

async fn http_bearer_as(
    f: &ParityFixture,
    address: SocketAddr,
    secret: &str,
    token: Option<&str>,
    body: Value,
    cert: &TestCert,
) -> (u16, Value) {
    let mut config = cert.client(&f.ca);
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let stream = TlsConnector::from(Arc::new(config))
        .connect(
            ServerName::try_from("localhost").unwrap(),
            TcpStream::connect(address).await.unwrap(),
        )
        .await
        .unwrap();
    let path = if token.is_some() {
        "/v1/session/execute"
    } else {
        "/v1/sessions"
    };
    let mut request = hyper::Request::builder()
        .method("POST")
        .uri(format!("https://localhost:{}{path}", address.port()))
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {secret}"));
    if let Some(token) = token {
        request = request.header("x-giggles-session", token);
    }
    let request = request
        .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
        .unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let reply = sender.send_request(request).await.unwrap();
    let status = reply.status().as_u16();
    let bytes = reply.into_body().collect().await.unwrap().to_bytes();
    task.abort();
    let _ = task.await;
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

fn sql_request(sql: &str) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: sql.into(),
            params: vec![],
        })),
        ..Default::default()
    }
}

async fn mapped_pg(f: &ParityFixture, certificates: BTreeMap<String, String>) -> Running {
    serve_postgresql(
        "127.0.0.1:0".parse().unwrap(),
        f.service.clone(),
        Arc::new(PostgresListener {
            tls: TlsMode::Required,
            tls_config: Some(f.tls.clone()),
            certificates,
            databases: vec!["giggles".into()],
            loopback_trust: false,
            connections: 8,
        }),
    )
    .await
    .unwrap()
}

async fn cert_refused(f: &ParityFixture, cert: &TestCert) {
    // Unmapped agent certificates fall back to the credential password path.
    assert_eq!(
        raw_startup(f.agent_pg.address, &f.ca, cert, parity::AGENT_STARTUP)
            .await
            .code(),
        Some("28P01")
    );
    let (status, body) = http_call(
        f.agent_http.address,
        (&f.ca, cert),
        "POST",
        "/v1/sessions",
        None,
        Some(&json!({"task": TASK})),
    )
    .await;
    assert_eq!(status, 401, "{body}");
    assert_eq!(body["error"]["code"], "UNAUTHENTICATED");
    assert_eq!(
        grpc_client(f.agent_grpc.address, &f.ca, cert)
            .await
            .open_session(proto::OpenSessionRequest {
                task: Some(TASK.into()),
                ..Default::default()
            })
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
}

#[tokio::test]
async fn agent_cert_on_legacy_postgres_native_http_grpc_refused() {
    let f = ParityFixture::start().await;
    let secret = issue(&f, TASK).await;
    assert_eq!(
        raw_startup(f.legacy_pg.address, &f.ca, &f.agent, parity::AGENT_STARTUP).await,
        Startup::Dropped
    );
    let native = async {
        let mut client = Client::connect(f.native.address, "localhost", Arc::new(f.agent.client(&f.ca))).await?;
        client.call(&Request::Ping).await
    }
    .await;
    assert!(native.is_err(), "agent identity must not enter framed listener");
    let (status, body) = http_call(
        f.http.address,
        (&f.ca, &f.agent),
        "POST",
        "/v1/sessions",
        None,
        Some(&json!({})),
    )
    .await;
    assert_eq!(status, 401, "{body}");
    assert_eq!(body["error"]["code"], "UNAUTHENTICATED");
    assert_eq!(
        grpc_client(f.grpc.address, &f.ca, &f.agent)
            .await
            .open_session(proto::OpenSessionRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert_eq!(
        f.password(EXECUTOR, &secret).await.unwrap_err().code().unwrap().code(),
        "28P01"
    );
    let (status, body) = http_bearer_as(&f, f.http.address, &secret, None, json!({}), &f.agent).await;
    assert_eq!(status, 401, "{body}");
    assert_eq!(body["error"]["code"], "UNAUTHENTICATED");
    // An agent cert plus its SQL secret is still not an ordinary API identity.
    let request = bearer(&secret, None, proto::OpenSessionRequest::default());
    assert_eq!(
        grpc_client(f.grpc.address, &f.ca, &f.agent)
            .await
            .open_session(request)
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    f.stop().await;
}

#[tokio::test]
async fn agent_cert_on_postgresql_listener_cert_map_refused() {
    let f = ParityFixture::start().await;
    // Even an explicit cert-map entry cannot turn a task-only executor into
    // an ordinary SQL login. Control proves the listener/map are operational.
    let pg = mapped_pg(
        &f,
        BTreeMap::from([
            (f.agent.fingerprint(), EXECUTOR.into()),
            (f.app.fingerprint(), APP.into()),
        ]),
    )
    .await;
    let refused = raw_startup(
        pg.address,
        &f.ca,
        &f.agent,
        &[("user", EXECUTOR), ("database", "giggles")],
    )
    .await;
    assert_eq!(refused.code(), Some("28P01"), "{refused:?}");
    let control = raw_startup(pg.address, &f.ca, &f.app, &[("user", APP), ("database", "giggles")]).await;
    assert!(matches!(control, Startup::Ready(_)), "{control:?}");
    let secret = issue(&f, TASK).await;
    assert_eq!(
        password_tls(&f, pg.address, &secret, None)
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "28P01"
    );
    pg.shutdown().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn executor_password_on_postgresql_listener_refused() {
    let f = ParityFixture::start().await;
    assert_eq!(
        f.password(EXECUTOR, EXECUTOR_PW)
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "28P01"
    );
    let app = f.password(APP, APP_PW).await.unwrap();
    app.simple_query("SELECT 1").await.unwrap();
    drop(app);
    f.stop().await;
}

#[tokio::test]
async fn ordinary_role_cert_and_application_cert_refused_on_agent_listeners() {
    let f = ParityFixture::start().await;
    cert_refused(&f, &f.app).await;
    let application = certificate(Some(&f.ca), false);
    let controller = certificate(Some(&f.ca), false);
    f.service
        .reload_bindings(vec![
            Binding {
                fingerprint: f.app.fingerprint(),
                role: Role::Database { name: APP.into() },
            },
            Binding {
                fingerprint: controller.fingerprint(),
                role: Role::Database {
                    name: CONTROLLER.into(),
                },
            },
            Binding {
                fingerprint: application.fingerprint(),
                role: Role::Application {
                    domain: f.admin.authority_metadata().unwrap().0.to_string(),
                    tables: [TABLE.into()].into(),
                },
            },
        ])
        .unwrap();
    cert_refused(&f, &controller).await;
    cert_refused(&f, &application).await;
    f.stop().await;
}

#[tokio::test]
async fn legacy_task_cert_refused_on_agent_listeners() {
    let f = ParityFixture::start().await;
    let legacy = certificate(Some(&f.ca), false);
    f.service
        .reload_bindings(vec![Binding {
            fingerprint: legacy.fingerprint(),
            role: Role::Task {
                domain: f.admin.authority_metadata().unwrap().0.to_string(),
                task: giggles_core::TaskId(91),
                principal: giggles_core::Principal(92),
            },
        }])
        .unwrap();
    cert_refused(&f, &legacy).await;
    f.stop().await;
}

#[tokio::test]
async fn task_session_cannot_approve_undo_define_or_revoke_tasks() {
    let f = ParityFixture::start().await;
    let secret = issue(&f, TASK).await;
    let digest = "00".repeat(32);
    let statements = [
        format!("APPROVE CHANGE 1 OF TASK \"{TASK}\" DIGEST '{digest}'"),
        format!("PREVIEW UNDO OF TASK \"{TASK}\""),
        format!("SHOW UNDO 1 OF TASK \"{TASK}\""),
        format!("APPROVE UNDO 1 OF TASK \"{TASK}\" DIGEST '{digest}' MODE COMPLETE"),
        inc_436_sql(&agent_support::timestamptz(f.expires_ms)),
        format!("REVOKE TASK \"{TASK}\""),
        format!("ISSUE CREDENTIAL FOR TASK \"{TASK}\""),
        "REVOKE CREDENTIAL 1".into(),
    ];
    for surface in Surface::ALL {
        for sql in &statements {
            let code = match surface {
                Surface::PgSimple | Surface::PgExtended => {
                    let client = password_tls(&f, f.agent_pg.address, &secret, None).await.unwrap();
                    let error = if surface == Surface::PgSimple {
                        client.simple_query(sql).await.unwrap_err()
                    } else {
                        client.query(sql, &[]).await.unwrap_err()
                    };
                    error.code().unwrap().code().to_owned()
                }
                Surface::HttpText => {
                    let (status, open) = http_bearer(&f, f.agent_http.address, &secret, None, json!({})).await;
                    assert_eq!(status, 201, "{open}");
                    let token = open["sessionId"].as_str().unwrap();
                    let (status, reply) = http_bearer(
                        &f,
                        f.agent_http.address,
                        &secret,
                        Some(token),
                        json!({"sql":{"sql":sql}}),
                    )
                    .await;
                    assert_eq!(status, 200, "{reply}");
                    reply["code"].as_str().unwrap().to_owned()
                }
                Surface::GrpcText => {
                    let mut client = grpc_client(f.agent_grpc.address, &f.ca, &f.app).await;
                    let token = client
                        .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
                        .await
                        .unwrap()
                        .into_inner()
                        .session_id;
                    let reply = client
                        .execute(bearer(&secret, Some(&token), sql_request(sql)))
                        .await
                        .unwrap()
                        .into_inner();
                    client
                        .close_session(bearer(&secret, Some(&token), proto::CloseSessionRequest {}))
                        .await
                        .unwrap();
                    reply.code
                }
            };
            assert_eq!(code, "0A000", "{surface:?}: {sql}");
        }
    }
    assert!(f.admin.tasks().get(TASK).unwrap().is_some());
    f.stop().await;
}

#[tokio::test]
async fn task_session_cannot_reach_legacy_giggles_json_or_request_sql() {
    let f = ParityFixture::start().await;
    let secret = issue(&f, TASK).await;
    let client = password_tls(&f, f.agent_pg.address, &secret, None).await.unwrap();
    for sql in ["GIGGLES {\"kind\":\"Ping\"}", "GIGGLES STATUS (1)"] {
        assert_eq!(
            client.simple_query(sql).await.unwrap_err().code().unwrap().code(),
            "0A000"
        );
        assert_eq!(
            client.query(sql, &[]).await.unwrap_err().code().unwrap().code(),
            "0A000"
        );
    }
    let (status, open) = http_bearer(&f, f.agent_http.address, &secret, None, json!({})).await;
    assert_eq!(status, 201);
    let token = open["sessionId"].as_str().unwrap();
    let mut grpc = grpc_client(f.agent_grpc.address, &f.ca, &f.app).await;
    let grpc_token = grpc
        .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .session_id;
    for body in [
        json!({"sql":{"sql":"GIGGLES {\"kind\":\"Ping\"}"}}),
        json!({"status":{"operation":"1"}}),
        json!({"preview":{"request":{"operation":"7","selection":{"keys":{"items":[{"tenant":"42","row":"3"}]}},"column":"COLUMN_STATUS","after":"DELIVERY_STATUS_QUEUED"}}}),
        json!({"execute":{"request":{"operation":"8","selection":{"keys":{"items":[{"tenant":"42","row":"3"}]}},"column":"COLUMN_STATUS","after":"DELIVERY_STATUS_QUEUED"}}}),
        json!({"repairSql":{"operation":"1","sql":"UPDATE shipments SET status = 'shipped'"}}),
    ] {
        let (status, reply) = http_bearer(&f, f.agent_http.address, &secret, Some(token), body.clone()).await;
        assert_eq!(status, 200, "{reply}");
        assert_eq!(reply["code"], "0A000", "{body}: {reply}");
        let request = wire_json::decode_execute(&serde_json::to_vec(&body).unwrap()).unwrap();
        let reply = grpc
            .execute(bearer(&secret, Some(&grpc_token), request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.code, "0A000", "{body}");
    }
    // NativeTextRequest has no legacy JSON/request-SQL grammar: decoding it
    // fails before any legacy invocation or SQL execution can be reached.
    for text in ["GIGGLES {\"kind\":\"Ping\"}", "SELECT id FROM shipments"] {
        let body = json!({"text":{"text":text,"languageVersion":LANGUAGE}});
        let (status, reply) = http_bearer(&f, f.agent_http.address, &secret, Some(token), body.clone()).await;
        assert_eq!(status, 400, "{reply}");
        let request = wire_json::decode_execute(&serde_json::to_vec(&body).unwrap()).unwrap();
        assert_eq!(
            grpc.execute(bearer(&secret, Some(&grpc_token), request))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    drop(client);
    f.stop().await;
}

#[tokio::test]
async fn set_role_and_session_authorization_from_any_role_cannot_enter_executor() {
    let f = ParityFixture::start().await;
    let secret = issue(&f, TASK).await;
    // A superuser joins the controller and application controls.
    f.admin
        .roles()
        .create(giggles_core::roles::NewRole {
            name: "bypass_admin".into(),
            login: true,
            superuser: true,
            password: Some(giggles_server::auth::scram::Verifier::generate("bypass-admin-pw")),
            ..Default::default()
        })
        .unwrap();
    for (role, password) in [
        ("bypass_admin", "bypass-admin-pw"),
        (CONTROLLER, CONTROLLER_PW),
        (APP, APP_PW),
    ] {
        let client = f.password(role, password).await.unwrap();
        for sql in [
            format!("SET ROLE {EXECUTOR}"),
            format!("SET SESSION AUTHORIZATION {EXECUTOR}"),
        ] {
            assert_eq!(
                client.simple_query(&sql).await.unwrap_err().code().unwrap().code(),
                "42704",
                "{role}: {sql}"
            );
        }
        client.simple_query("SELECT 1").await.unwrap();
    }
    let client = password_tls(&f, f.agent_pg.address, &secret, None).await.unwrap();
    for sql in [
        format!("SET ROLE {APP}"),
        format!("SET SESSION AUTHORIZATION {CONTROLLER}"),
    ] {
        assert_eq!(
            client.simple_query(&sql).await.unwrap_err().code().unwrap().code(),
            "0A000"
        );
        assert_eq!(
            client.query(&sql, &[]).await.unwrap_err().code().unwrap().code(),
            "0A000"
        );
    }
    drop(client);
    f.stop().await;
}

#[tokio::test]
async fn api_session_token_from_ordinary_listener_unusable_on_agent_and_reverse() {
    let f = ParityFixture::start().await;
    let secret = issue(&f, TASK).await;
    let (status, ordinary) = http_call(
        f.http.address,
        (&f.ca, &f.app),
        "POST",
        "/v1/sessions",
        None,
        Some(&json!({})),
    )
    .await;
    assert_eq!(status, 201);
    let ordinary = ordinary["sessionId"].as_str().unwrap();
    let (status, agent) = http_bearer(&f, f.agent_http.address, &secret, None, json!({})).await;
    assert_eq!(status, 201);
    let agent = agent["sessionId"].as_str().unwrap();
    let (status, body) = http_bearer(&f, f.agent_http.address, &secret, Some(ordinary), json!({"ping":{}})).await;
    assert_eq!(status, 404, "{body}");
    let (status, body) = http_call(
        f.http.address,
        (&f.ca, &f.app),
        "POST",
        "/v1/session/execute",
        Some(agent),
        Some(&json!({"ping":{}})),
    )
    .await;
    assert_eq!(status, 404, "{body}");
    let mut normal = grpc_client(f.grpc.address, &f.ca, &f.app).await;
    let mut task = grpc_client(f.agent_grpc.address, &f.ca, &f.app).await;
    let normal_token = normal
        .open_session(proto::OpenSessionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .session_id;
    let task_token = task
        .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .session_id;
    let ping = || proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
        ..Default::default()
    };
    assert_eq!(
        task.execute(bearer(&secret, Some(&normal_token), ping()))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        normal
            .execute(with_session(&task_token, ping()))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    // Original tokens still resolve on their owning runtimes.
    assert_eq!(
        normal
            .execute(with_session(&normal_token, ping()))
            .await
            .unwrap()
            .into_inner()
            .code,
        "ok"
    );
    assert_eq!(
        task.execute(bearer(&secret, Some(&task_token), ping()))
            .await
            .unwrap()
            .into_inner()
            .code,
        "ok"
    );
    f.stop().await;
}

#[tokio::test]
async fn second_task_of_same_binding_needs_new_session() {
    let f = ParityFixture::start().await;
    let second = "inc-437";
    let sql = inc_436_sql(&agent_support::timestamptz(f.expires_ms))
        .replacen(TASK, second, 1)
        .replace("merchant_id = 42", "merchant_id = 43");
    let Some(giggles_core::task::statements::TaskStatement::Create(new)) = giggles_core::task::statements::parse(&sql)
    else {
        panic!("task definition");
    };
    let issuer = f.admin.roles().get(CONTROLLER).unwrap().unwrap();
    f.admin
        .tasks()
        .create(
            &issuer,
            *new,
            giggles_core::task::definition::WallClockMs(agent_support::now_ms()),
        )
        .unwrap();
    f.service
        .reload_all_bindings(
            vec![Binding {
                fingerprint: f.app.fingerprint(),
                role: Role::Database { name: APP.into() },
            }],
            vec![binding(&f.agent, EXECUTOR, &[TASK, second])],
        )
        .unwrap();
    let unselected = raw_startup(
        f.agent_pg.address,
        &f.ca,
        &f.agent,
        &[("user", EXECUTOR), ("database", "giggles")],
    )
    .await;
    assert_eq!(unselected.code(), Some("28000"));
    let mut first = f.agent_wire().await;
    assert_eq!(
        error_of(&first.simple("SET giggles.task = 'inc-437'").await).unwrap().0,
        "0A000"
    );
    let first_rows = first.simple("SELECT id FROM shipments").await;
    assert!(error_of(&first_rows).is_none(), "{first_rows:?}");
    assert_eq!(
        first_rows.iter().filter(|m| matches!(m, Msg::Row(_))).count(),
        MERCHANT_42_ROWS
    );
    let mut other = agent_support::connect_raw(
        f.agent_pg.address,
        &f.ca,
        &f.agent,
        &[("user", EXECUTOR), ("database", "giggles"), ("giggles.task", second)],
    )
    .await;
    let second_rows = other.simple("SELECT id FROM shipments").await;
    assert!(error_of(&second_rows).is_none(), "{second_rows:?}");
    assert_eq!(
        second_rows.iter().filter(|m| matches!(m, Msg::Row(_))).count(),
        MERCHANT_43_ROWS
    );
    // SQL credentials each bind exactly one task, unlike a certificate map.
    let secret = issue(&f, TASK).await;
    assert_eq!(
        password_tls(&f, f.agent_pg.address, &secret, Some(second))
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "28P01"
    );
    let second_secret = issue(&f, second).await;
    let client = password_tls(&f, f.agent_pg.address, &second_secret, Some(second))
        .await
        .unwrap();
    let rows = client.simple_query("SELECT id FROM shipments").await.unwrap();
    assert_eq!(
        rows.iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count(),
        MERCHANT_43_ROWS
    );
    drop((client, first, other));
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_request_with_foreign_key_does_not_cancel_agent_backend() {
    use giggles_core::task::ProtectedPoint;
    use std::sync::Mutex;
    let f = ParityFixture::start().await;
    let secret = issue(&f, TASK).await;
    let agent = password_tls(&f, f.agent_pg.address, &secret, None).await.unwrap();
    let foreign = password_tls(&f, f.agent_pg.address, &secret, None).await.unwrap();
    let ordinary = f.password(APP, APP_PW).await.unwrap();
    let (entered, wait) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, hold) = std::sync::mpsc::channel();
    let hold = Mutex::new(hold);
    f.admin
        .set_protected_hook(Some(Arc::new(move |point| {
            if point == ProtectedPoint::AfterLookup
                && let Some(entered) = entered.lock().unwrap().take()
            {
                entered.send(()).unwrap();
                hold.lock().unwrap().recv_timeout(Duration::from_secs(8)).unwrap();
            }
        })))
        .unwrap();
    let cancel_foreign = foreign.cancel_token();
    let cancel_ordinary = ordinary.cancel_token();
    let cancel = async {
        tokio::time::timeout(Duration::from_secs(8), wait)
            .await
            .unwrap()
            .unwrap();
        // The agent statement is paused inside core, with its cancellation
        // context installed. Both tokens contain real, different backend keys.
        let agent_cancel = cancel_foreign
            .cancel_query(MakeRustlsConnect::new(f.app.client(&f.ca)))
            .await;
        let ordinary_cancel = cancel_ordinary.cancel_query(tokio_postgres::NoTls).await;
        release.send(()).unwrap();
        agent_cancel.unwrap();
        ordinary_cancel.unwrap();
    };
    let (result, ()) = tokio::join!(
        agent.simple_query("OPERATION 'foreign-cancel' UPDATE shipments SET status = 'queued' WHERE id = 1"),
        cancel,
    );
    f.admin.set_protected_hook(None).unwrap();
    let result = result.unwrap();
    assert!(
        result
            .iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(1))),
        "{result:?}"
    );
    // Unknown keys on the agent endpoint are silent and harmless as well.
    let mut socket = TcpStream::connect(f.agent_pg.address).await.unwrap();
    socket.write_all(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f]).await.unwrap();
    assert_eq!(socket.read_u8().await.unwrap(), b'S');
    let mut socket = TlsConnector::from(Arc::new(f.app.client(&f.ca)))
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap();
    let mut packet = 16u32.to_be_bytes().to_vec();
    packet.extend(80877102u32.to_be_bytes());
    packet.extend(i32::MAX.to_be_bytes());
    packet.extend(i32::MIN.to_be_bytes());
    socket.write_all(&packet).await.unwrap();
    drop(socket);
    agent.simple_query("SELECT id FROM shipments").await.unwrap();
    drop((agent, foreign, ordinary));
    f.stop().await;
}
