//! R3-205 (GD-0061 design-2 sections 1 and 3): the agent pgwire listener, its
//! startup policy and the `agent` config section, over real TLS.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{
    AgentServer, Startup, admin, binding, certificate, create_task, raw_startup, revoke,
};
use giggles_server::{
    Binding, Request, Role, agent::AgentConfig, pgwire::AGENT_REFUSAL, serve_bound,
    serve_pg_bound, transport::Client,
};
use tokio::sync::watch;
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;

fn user(task: &str) -> Vec<(&'static str, String)> {
    vec![
        ("user", "agent_exec".into()),
        ("database", "giggles".into()),
        ("giggles.task", task.into()),
    ]
}

async fn start(
    server: &AgentServer,
    cert: &agent_support::TestCert,
    params: &[(&str, &str)],
) -> Startup {
    raw_startup(server.agent.address, &server.ca, cert, params).await
}

fn assert_refused(startup: &Startup) {
    match startup {
        Startup::Error {
            severity,
            code,
            message,
            ..
        } => {
            assert_eq!(
                (severity.as_str(), code.as_str(), message.as_str()),
                ("FATAL", "28000", AGENT_REFUSAL)
            );
        }
        other => panic!("expected the agent refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn connects_with_task_parameter_and_with_options_form() {
    let admin = admin();
    create_task(&admin, "inc-436", "agent_exec");
    let ca = certificate(None, false);
    let agent = certificate(Some(&ca), false);
    let server = AgentServer::start(
        admin,
        ca,
        vec![],
        vec![binding(&agent, "agent_exec", &["inc-436"])],
    )
    .await;
    let direct = start(
        &server,
        &agent,
        &[
            ("user", "agent_exec"),
            ("database", "giggles"),
            ("giggles.task", "inc-436"),
        ],
    )
    .await;
    assert_eq!(direct.status("session_authorization"), Some("agent_exec"));
    assert_eq!(direct.status("is_superuser"), Some("off"));
    // A binding with one task needs no selection.
    let implicit = start(&server, &agent, &[("user", "agent_exec"), ("database", "giggles")]).await;
    assert_eq!(implicit.status("session_authorization"), Some("agent_exec"));
    // A real driver sends the task in `options`.
    let (client, connection) = tokio_postgres::Config::new()
        .host("localhost")
        .hostaddr(server.agent.address.ip())
        .port(server.agent.address.port())
        .user("agent_exec")
        .dbname("giggles")
        .options("-c giggles.task=inc-436")
        .ssl_mode(SslMode::Require)
        .connect(MakeRustlsConnect::new(agent.client(&server.ca)))
        .await
        .expect("driver connects with the options form");
    let driver = tokio::spawn(connection);
    assert!(!client.is_closed());
    drop(client);
    let _ = driver.await;
    // Any other `options` is refused before credentials are looked at.
    let other = start(
        &server,
        &agent,
        &[
            ("user", "agent_exec"),
            ("database", "giggles"),
            ("options", "-c search_path=public"),
        ],
    )
    .await;
    assert_eq!(other.code(), Some("0A000"));
    let unknown_database = start(
        &server,
        &agent,
        &[("user", "agent_exec"), ("database", "postgres")],
    )
    .await;
    assert_eq!(unknown_database.code(), Some("3D000"));
}

#[tokio::test]
async fn refused_without_task_when_binding_has_two() {
    let admin = admin();
    create_task(&admin, "inc-436", "agent_exec");
    create_task(&admin, "inc-437", "agent_exec");
    let ca = certificate(None, false);
    let agent = certificate(Some(&ca), false);
    let server = AgentServer::start(
        admin,
        ca,
        vec![],
        vec![binding(&agent, "agent_exec", &["inc-436", "inc-437"])],
    )
    .await;
    assert_refused(&start(&server, &agent, &[("user", "agent_exec"), ("database", "giggles")]).await);
    for task in ["inc-436", "inc-437"] {
        let params = user(task);
        let params: Vec<_> = params.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let ok = start(&server, &agent, &params).await;
        assert_eq!(ok.status("session_authorization"), Some("agent_exec"), "{task}");
    }
}

#[tokio::test]
async fn refused_for_wrong_user_unknown_cert_ordinary_cert_revoked_task() {
    let admin = admin();
    create_task(&admin, "inc-436", "agent_exec");
    create_task(&admin, "inc-revoked", "agent_exec");
    revoke(&admin, "inc-revoked");
    let ca = certificate(None, false);
    let agent = certificate(Some(&ca), false);
    let ordinary = certificate(Some(&ca), false);
    let unknown = certificate(Some(&ca), false);
    let server = AgentServer::start(
        admin,
        ca,
        vec![Binding {
            fingerprint: ordinary.fingerprint(),
            role: Role::Database {
                name: "oncall_lead".into(),
            },
        }],
        vec![binding(&agent, "agent_exec", &["inc-436", "inc-revoked"])],
    )
    .await;
    // The executor of another binding, or an ordinary role, is not this one.
    for name in ["other_exec", "oncall_lead", "giggles"] {
        assert_refused(
            &start(
                &server,
                &agent,
                &[("user", name), ("database", "giggles"), ("giggles.task", "inc-436")],
            )
            .await,
        );
    }
    // Certificates outside the agent map are dropped before any message.
    for cert in [&unknown, &ordinary] {
        let got = start(
            &server,
            cert,
            &[("user", "agent_exec"), ("database", "giggles"), ("giggles.task", "inc-436")],
        )
        .await;
        assert_eq!(got, Startup::Dropped);
    }
    let revoked = user("inc-revoked");
    let revoked: Vec<_> = revoked.iter().map(|(k, v)| (*k, v.as_str())).collect();
    assert_refused(&start(&server, &agent, &revoked).await);
    // The live task on the same certificate still connects.
    let live = user("inc-436");
    let live: Vec<_> = live.iter().map(|(k, v)| (*k, v.as_str())).collect();
    assert_eq!(
        start(&server, &agent, &live).await.status("session_authorization"),
        Some("agent_exec")
    );
}

#[tokio::test]
async fn refusals_share_one_fatal_message() {
    let admin = admin();
    create_task(&admin, "inc-436", "agent_exec");
    create_task(&admin, "inc-437", "agent_exec");
    create_task(&admin, "inc-other", "other_exec");
    create_task(&admin, "inc-revoked", "agent_exec");
    revoke(&admin, "inc-revoked");
    let ca = certificate(None, false);
    let agent = certificate(Some(&ca), false);
    let server = AgentServer::start(
        admin,
        ca,
        vec![],
        vec![binding(
            &agent,
            "agent_exec",
            &["inc-436", "inc-437", "inc-revoked", "inc-missing", "inc-other"],
        )],
    )
    .await;
    let cases: [&[(&str, &str)]; 6] = [
        // No task selected while the binding lists several.
        &[("user", "agent_exec"), ("database", "giggles")],
        // A task the binding does not list.
        &[("user", "agent_exec"), ("database", "giggles"), ("giggles.task", "inc-999")],
        // Revoked, never created, another executor's task.
        &[("user", "agent_exec"), ("database", "giggles"), ("giggles.task", "inc-revoked")],
        &[("user", "agent_exec"), ("database", "giggles"), ("options", "-c giggles.task=inc-missing")],
        &[("user", "agent_exec"), ("database", "giggles"), ("giggles.task", "inc-other")],
        // The wrong startup user.
        &[("user", "other_exec"), ("database", "giggles"), ("giggles.task", "inc-436")],
    ];
    let mut bodies = Vec::new();
    for params in cases {
        let got = start(&server, &agent, params).await;
        assert_refused(&got);
        let Startup::Error { body, .. } = got else {
            unreachable!()
        };
        bodies.push(body);
    }
    assert!(
        bodies.windows(2).all(|w| w[0] == w[1]),
        "every refusal carries byte-identical fields"
    );
}

#[tokio::test]
async fn agent_cert_refused_on_legacy_postgres_native_listeners() {
    let admin = admin();
    create_task(&admin, "inc-436", "agent_exec");
    let ca = certificate(None, false);
    let agent = certificate(Some(&ca), false);
    let ordinary = certificate(Some(&ca), false);
    let server = AgentServer::start(
        admin,
        ca,
        vec![Binding {
            fingerprint: ordinary.fingerprint(),
            role: Role::Database {
                name: "oncall_lead".into(),
            },
        }],
        vec![binding(&agent, "agent_exec", &["inc-436"])],
    )
    .await;
    let (_open, gate) = watch::channel(true);
    let legacy_pg = serve_pg_bound(
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        server.service.clone(),
        server.tls.clone(),
        8,
        gate.clone(),
    )
    .await
    .unwrap();
    let native = serve_bound(
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        server.service.clone(),
        server.tls.clone(),
        8,
        gate,
    )
    .await
    .unwrap();
    // The legacy certificate pgwire listener: same TLS, ordinary map only.
    for params in [
        &[("user", "giggles"), ("database", "giggles")][..],
        &[("user", "agent_exec"), ("database", "giggles"), ("giggles.task", "inc-436")][..],
    ] {
        assert_eq!(
            raw_startup(legacy_pg.address, &server.ca, &agent, params).await,
            Startup::Dropped
        );
    }
    // The native framed listener drops it as well; an ordinary certificate works.
    let call = |cert: &agent_support::TestCert| {
        let config = std::sync::Arc::new(cert.client(&server.ca));
        let address = native.address;
        async move {
            let mut client = Client::connect(address, "localhost", config).await?;
            client.call(&Request::Ping).await
        }
    };
    assert!(call(&agent).await.is_err());
    assert_eq!(call(&ordinary).await.unwrap().code, "ok");
    // The ordinary certificate is in turn dropped by the agent listener.
    assert_eq!(
        start(&server, &ordinary, &[("user", "oncall_lead"), ("database", "giggles")]).await,
        Startup::Dropped
    );
    native.shutdown().await.unwrap();
    legacy_pg.shutdown().await.unwrap();
}

#[test]
fn config_rejects_unknown_key_and_empty_agent() {
    let parse = |json: serde_json::Value| serde_json::from_value::<AgentConfig>(json);
    // An empty section names no listener.
    assert!(parse(serde_json::json!({})).unwrap().validate().is_err());
    assert!(
        parse(serde_json::json!({"postgres": "127.0.0.1:0", "listen": "127.0.0.1:0"})).is_err()
    );
    assert!(
        parse(serde_json::json!({"postgres": "127.0.0.1:0", "bindings": [
            {"fingerprint": "aa", "executor": "x", "tasks": ["t"], "role": "x"}
        ]}))
        .is_err()
    );
    let config = parse(serde_json::json!({"postgres": "127.0.0.1:6543"})).unwrap();
    config.validate().unwrap();
    assert_eq!(config.databases, ["giggles"]);
    assert_eq!(config.connections, 32);
    assert!(config.client_ca.is_none() && config.bindings.is_empty());
    for bad in [
        serde_json::json!({"postgres": "127.0.0.1:0", "connections": 0}),
        serde_json::json!({"postgres": "127.0.0.1:0", "connections": 129}),
        serde_json::json!({"postgres": "127.0.0.1:0", "databases": []}),
        serde_json::json!({"postgres": "127.0.0.1:0", "databases": [""]}),
        serde_json::json!({"postgres": "127.0.0.1:0", "databases": ["a", "a"]}),
        serde_json::json!({"postgres": "127.0.0.1:0", "client_ca": ""}),
    ] {
        assert!(parse(bad.clone()).unwrap().validate().is_err(), "{bad}");
    }
    // `bindings_file` wins over `bindings`.
    let dir = agent_support::scratch("agent-config");
    let file = dir.join("agent-bindings.json");
    let from_file = serde_json::json!([
        {"fingerprint": "b".repeat(64), "executor": "agent_exec", "tasks": ["inc-436"]}
    ]);
    std::fs::write(&file, from_file.to_string()).unwrap();
    let config = parse(serde_json::json!({
        "postgres": "127.0.0.1:0",
        "bindings": [{"fingerprint": "a".repeat(64), "executor": "agent_exec", "tasks": ["inc-436"]}],
        "bindings_file": file.to_str().unwrap(),
    }))
    .unwrap();
    let loaded = config.load_bindings().unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].fingerprint, "b".repeat(64));
    std::fs::remove_dir_all(dir).unwrap();
}
