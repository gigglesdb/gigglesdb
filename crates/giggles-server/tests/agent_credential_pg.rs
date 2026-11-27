//! R3-1305: SQL credentials on the TLS agent PostgreSQL listener.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;
use agent_support::*;
use giggles_server::{Binding, Role};
use std::time::Duration;
use tokio_postgres::{Client, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

struct Fixture {
    server: AgentServer,
    ordinary: giggles_server::Running,
    issuer: Wire,
}
impl Fixture {
    async fn start() -> Self { Self::start_limit(8).await }
    async fn start_limit(limit: usize) -> Self {
        let admin = admin();
        admin
            .roles()
            .grant_table(
                "shipments",
                "agent_exec",
                &[giggles_core::roles::TablePrivilege::Select],
            )
            .unwrap();
        admin
            .sql_session()
            .execute(
                "INSERT INTO shipments VALUES (1,42,'pending'),(2,7,'hidden')",
                &[],
                None,
            )
            .unwrap();
        create_task(&admin, "inc-436", "agent_exec");
        create_task(&admin, "inc-437", "agent_exec");
        let ca = certificate(None, false);
        let issuer = certificate(Some(&ca), false);
        let server = AgentServer::start_limit(
            admin,
            ca,
            vec![Binding {
                fingerprint: issuer.fingerprint(),
                role: Role::Database {
                    name: "incident_controller".into(),
                },
            }],
            vec![],
            limit,
        )
        .await;
        let (ordinary, issuer) = ordinary_wire(&server, &issuer).await;
        Self {
            server,
            ordinary,
            issuer,
        }
    }
    async fn issue(&mut self, expiry: Option<i64>) -> (String, String) {
        let mut sql = "ISSUE CREDENTIAL FOR TASK \"inc-436\"".to_owned();
        if let Some(expiry) = expiry {
            sql.push_str(&format!(" EXPIRES '{}'", timestamptz(expiry)));
        }
        let replies = self.issuer.simple(&sql).await;
        let row = replies
            .iter()
            .find_map(|m| match m {
                Msg::Row(row) => Some(row),
                _ => None,
            })
            .unwrap_or_else(|| panic!("issue failed: {replies:?}"));
        (row[0].clone().unwrap(), row[2].clone().unwrap())
    }
    async fn connect(
        &self,
        user: &str,
        secret: &str,
        task: Option<&str>,
    ) -> Result<Client, tokio_postgres::Error> {
        let mut config = tokio_postgres::Config::new();
        config
            .host("localhost")
            .hostaddr(self.server.agent.address.ip())
            .port(self.server.agent.address.port())
            .user(user)
            .password(secret)
            .dbname("giggles")
            .ssl_mode(SslMode::Require)
            .connect_timeout(Duration::from_secs(5));
        if let Some(task) = task {
            config.options(format!("-c giggles.task={task}"));
        }
        let (client, connection) = config
            .connect(MakeRustlsConnect::new(no_cert_client(&self.server.ca)))
            .await?;
        tokio::spawn(connection);
        Ok(client)
    }
    async fn refusal(&self, user: &str, secret: &str, task: Option<&str>) -> (String, String) {
        let error = self
            .connect(user, secret, task)
            .await
            .expect_err("must refuse");
        let error = error.as_db_error().expect("database error");
        assert_eq!(error.code().code(), "28P01");
        assert_eq!(
            error.message(),
            format!("password authentication failed for user \"{user}\"")
        );
        (error.code().code().into(), error.message().into())
    }
    async fn stop(self) {
        drop(self.issuer);
        self.ordinary.shutdown().await.unwrap();
        self.server.agent.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn connect_select_revoke_cuts_and_reconnect_fails() {
    let mut f = Fixture::start().await;
    let (id, secret) = f.issue(None).await;
    let client = f.connect("agent_exec", &secret, None).await.unwrap();
    let rows = client
        .simple_query("SELECT id,status FROM shipments")
        .await
        .unwrap();
    let visible: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((row.get(0), row.get(1))),
            _ => None,
        })
        .collect();
    assert_eq!(visible, vec![(Some("1"), Some("pending"))]);
    let revoked = f.issuer.simple(&format!("REVOKE CREDENTIAL {id}")).await;
    assert!(
        !revoked.iter().any(|m| matches!(m, Msg::Error { .. })),
        "{revoked:?}"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while !client.is_closed() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("revocation closes idle connection");
    assert!(
        client
            .simple_query("SELECT id FROM shipments")
            .await
            .is_err()
    );
    f.refusal("agent_exec", &secret, None).await;
    drop(client);
    f.stop().await;
}
#[tokio::test]
async fn wrong_secret_is_28p01() {
    let mut f = Fixture::start().await;
    let (_, secret) = f.issue(None).await;
    let mut wrong = secret.clone().into_bytes();
    let last = wrong.last_mut().unwrap();
    *last = if *last == b'0' { b'1' } else { b'0' };
    f.refusal("agent_exec", &String::from_utf8(wrong).unwrap(), None)
        .await;
    f.refusal("agent_exec", "", None).await;
    f.stop().await;
}
#[tokio::test]
async fn unknown_id_is_28p01_same_bytes() {
    let mut f = Fixture::start().await;
    let (_, secret) = f.issue(None).await;
    let wrong = format!("{}_{}", &secret[..22], "0".repeat(64));
    let unknown = format!("ggtc1_ffffffffffffffff_{}", "0".repeat(64));
    assert_eq!(
        f.refusal("agent_exec", &wrong, None).await,
        f.refusal("agent_exec", &unknown, None).await
    );
    f.stop().await;
}
#[tokio::test]
async fn wrong_user_is_28p01() {
    let mut f = Fixture::start().await;
    let (_, secret) = f.issue(None).await;
    f.refusal("other_exec", &secret, None).await;
    f.stop().await;
}
#[tokio::test]
async fn secret_for_task_a_cannot_open_task_b() {
    let mut f = Fixture::start().await;
    let (_, secret) = f.issue(None).await;
    f.refusal("agent_exec", &secret, Some("inc-437")).await;
    let client = f
        .connect("agent_exec", &secret, Some("inc-436"))
        .await
        .unwrap();
    drop(client);
    f.stop().await;
}
#[tokio::test]
async fn expired_credential_is_28p01() {
    let mut f = Fixture::start().await;
    let expiry = now_ms() + 2000;
    let (_, secret) = f.issue(Some(expiry)).await;
    tokio::time::sleep(Duration::from_millis(
        (expiry - now_ms()).max(0) as u64 + 100,
    ))
    .await;
    f.refusal("agent_exec", &secret, None).await;
    f.stop().await;
}
#[tokio::test]
async fn cert_binding_still_works() {
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
    let startup = raw_startup(
        server.agent.address,
        &server.ca,
        &agent,
        &[("user", "agent_exec"), ("database", "giggles")],
    )
    .await;
    assert_eq!(startup.status("session_authorization"), Some("agent_exec"));
    server.agent.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_sql_credential_listener_metadata_preserves_task_policy() {
    let mut f = Fixture::start_limit(7).await;
    let (_, secret) = f.issue(None).await;
    let client = f.connect("agent_exec", &secret, None).await.unwrap();
    assert_agent_settings_refused(&client).await;
    drop(client);
    f.stop().await;
}
