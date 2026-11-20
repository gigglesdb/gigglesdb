//! Qualified task targets retain their schema through binding and dependency checks.
#![cfg(feature = "engine-rocksdb")]
mod r3_support;
use r3_support::{R3Fixture, agent_support::timestamptz};
use tokio_postgres::{Client, SimpleQueryMessage};

fn setup(f: &R3Fixture) {
    f.admin().roles().create(giggles_core::roles::NewRole {
        name: "qualified_agent".into(), login: true, ..Default::default()
    }).unwrap();
    f.admin().roles().set_task_only("qualified_agent", true).unwrap();
    for sql in [
        "CREATE SCHEMA s",
        "CREATE SCHEMA s2",
        "CREATE TABLE s.t (id bigint PRIMARY KEY, tenant bigint, status text, secret text, unused text)",
        "INSERT INTO s.t VALUES (1, 42, 'failed', 'private', 'x'), (2, 7, 'failed', 'other', 'y')",
        "CREATE TABLE public.t (id bigint PRIMARY KEY, tenant bigint, status text, secret text, unused text)",
        "INSERT INTO public.t VALUES (9, 42, 'failed', 'public', 'z')",
        "CREATE TABLE s2.t (id bigint PRIMARY KEY)",
        "CREATE VIEW s.target_view AS SELECT id FROM s.t",
        "CREATE TABLE s.\"T\" (id bigint PRIMARY KEY)",
        "CREATE SCHEMA \"S\"",
        "CREATE TABLE \"S\".t (id bigint PRIMARY KEY)",
    ] {
        f.sql(sql);
    }
    f.admin().roles().grant_schema("s", "qualified_agent", &[giggles_core::roles::SchemaPrivilege::Usage]).unwrap();
    for target in ["s\0t", "t"] {
        f.admin()
            .roles()
            .set_table_owner(target, "incident_controller")
            .unwrap();
        f.admin()
            .roles()
            .grant_table(
                target,
                "qualified_agent",
                &[
                    giggles_core::roles::TablePrivilege::Select,
                    giggles_core::roles::TablePrivilege::Update,
                ],
            )
            .unwrap();
    }
}
async fn agent(f: &R3Fixture) -> (Client, giggles_server::Running) {
    use r3_support::agent_support::binding;
    let service = giggles_server::Service::with_agent(
        f.admin().clone(), Vec::new(),
        vec![binding(&f.agent_cert, "qualified_agent", &["inc-436"])],
        giggles_server::clock::Clock::new(0, 3600).unwrap(),
        giggles_server::admission::ServiceLimits::default(),
    ).unwrap();
    let tls = giggles_server::tls::server(
        vec![f.server_cert.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(f.server_cert.key.serialize_der()).into(),
        vec![f.ca.cert.der().clone()],
    ).unwrap();
    let (_open, start) = tokio::sync::watch::channel(true);
    let listener = giggles_server::agent::serve_agent_pg_bound(
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        service, tls, 16, vec!["giggles".into()], start,
    ).await.unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .host("localhost").hostaddr(listener.address.ip()).port(listener.address.port())
        .user("qualified_agent").dbname("giggles").options("-c giggles.task=inc-436")
        .ssl_mode(tokio_postgres::config::SslMode::Require)
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(f.agent_cert.client(&f.ca)))
        .await.unwrap();
    tokio::spawn(connection);
    (client, listener)
}
fn create_sql(f: &R3Fixture, target: &str) -> String {
    format!(
        "CREATE TASK \"inc-436\" EXECUTOR qualified_agent ON {target} \
        VISIBLE ROWS WHERE (tenant = 42) READABLE (id, status) MASKED (secret) \
        ALLOW UPDATE (status) OLD ROW WHERE (status = 'failed') NEW ROW WHERE (status = 'queued') \
        AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS APPROVE ABOVE 25 \
        DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
        EXPIRES '{}'",
        timestamptz(f.expires_ms)
    )
}
async fn rows(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect())
            }
            _ => None,
        })
        .collect()
}
async fn target_rows(client: &Client, target: &str) {
    let result = rows(client, &format!("SELECT * FROM {target} ORDER BY id")).await;
    assert_eq!(
        result,
        vec![vec![Some("1".into()), Some("failed".into()), None]]
    );
}
#[tokio::test]
async fn qualified_target_read_write_search_path_and_isolation() {
    let f = R3Fixture::start().await;
    setup(&f);
    let issuer = f.controller().await;
    issuer.simple_query(&create_sql(&f, "s.t")).await.unwrap();
    assert!(!rows(&issuer, "SHOW TASK \"inc-436\"").await.is_empty());
    let (agent, listener) = agent(&f).await;
    target_rows(&agent, "s.t").await;
    target_rows(&agent, "\"s\".\"t\"").await;
    let error = agent.simple_query("SET search_path = s").await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "0A000");
    let other_set = agent.simple_query("SET role = qualified_agent").await.unwrap_err();
    assert_eq!(error.code(), other_set.code());
    assert_eq!(error.as_db_error().unwrap().message(), other_set.as_db_error().unwrap().message());
    for target in ["t", "public.t", "s2.t", "s.target_view", "\"S\".\"t\"", "s.\"T\""] {
        let error = agent.simple_query(&format!("SELECT id FROM {target}")).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "42501", "{target}: {error}");
        assert!(error.as_db_error().unwrap().message().contains("outside this task"));
    }
    let error = agent.simple_query("PREVIEW UPDATE public.t SET status = 'queued'").await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "42501");
    f.sql("CREATE SCHEMA qualified_agent");
    f.sql("CREATE TABLE qualified_agent.t (id bigint PRIMARY KEY)");
    f.sql("INSERT INTO qualified_agent.t VALUES (88)");
    let error = agent.simple_query("SELECT id FROM t").await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "42501");
    target_rows(&agent, "s.t").await;
    for sql in [
        "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND n.nspname IN ('public', 's', 's2', 'S') ORDER BY n.nspname, c.relname",
        "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname IN ('public', 's', 's2', 'S') ORDER BY schemaname, tablename",
        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('public', 's', 's2', 'S') ORDER BY table_schema, table_name",
    ] {
        assert_eq!(
            rows(&agent, sql).await,
            vec![vec![Some("s".into()), Some("t".into())]],
            "{sql}"
        );
    }
    let preview = rows(
        &agent,
        "PREVIEW UPDATE s.t SET status = 'queued' WHERE id = 1",
    )
    .await;
    let change = preview[0][0].as_ref().unwrap();
    let digest = preview[0][1].as_ref().unwrap();
    agent
        .simple_query(&format!(
            "OPERATION 'q1' APPLY CHANGE {change} DIGEST '{digest}'"
        ))
        .await
        .unwrap();
    assert_eq!(
        rows(&agent, "SELECT status FROM s.t WHERE id = 1").await,
        vec![vec![Some("queued".into())]]
    );
    listener.shutdown().await.unwrap();
}
#[tokio::test]
async fn qualified_target_column_dependency_guard() {
    let f = R3Fixture::start().await;
    setup(&f);
    let issuer = f.controller().await;
    issuer.simple_query(&create_sql(&f, "s.t")).await.unwrap();
    f.sql("ALTER TABLE s.t DROP COLUMN unused");
    // Use the administrator session so authorization cannot mask the dependency error.
    let error = f
        .admin()
        .sql_session()
        .execute("ALTER TABLE s.t DROP COLUMN tenant", &[], None)
        .unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "2BP01", "{error:?}");
}
#[tokio::test]
async fn unqualified_task_target_still_resolves_in_public() {
    let f = R3Fixture::start().await;
    setup(&f);
    let issuer = f.controller().await;
    issuer.simple_query("SET search_path = s").await.unwrap();
    issuer.simple_query(&create_sql(&f, "t")).await.unwrap();
    let (agent, listener) = agent(&f).await;
    assert_eq!(
        rows(&agent, "SELECT id FROM public.t").await,
        vec![vec![Some("9".into())]]
    );
    assert_eq!(rows(&agent, "SELECT id FROM t").await, vec![vec![Some("9".into())]]);
    f.sql("CREATE SCHEMA qualified_agent");
    f.sql("CREATE TABLE qualified_agent.t (id bigint PRIMARY KEY)");
    f.sql("INSERT INTO qualified_agent.t VALUES (88)");
    let error = agent.simple_query("SELECT id FROM t").await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "42501");
    assert!(error.as_db_error().unwrap().message().contains("outside this task"));
    assert_eq!(rows(&agent, "SELECT id FROM public.t").await, vec![vec![Some("9".into())]]);
    let error = agent.simple_query("SELECT id FROM s.t").await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "42501");
    listener.shutdown().await.unwrap();
}
