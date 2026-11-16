//! GD-0077 E3-3: invalidated idle sessions release locks without another request.
mod api_support;

use api_support::{Fixture as ApiFixture, rpc};
use giggles_core::roles::NewRole;
use giggles_server::{
    Role, Running,
    api::json as wire_json,
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{sync::oneshot, time::timeout};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, config::SslMode};

const PROMPT: Duration = Duration::from_secs(5);
type Closed = oneshot::Receiver<Result<(), tokio_postgres::Error>>;

struct Fixture {
    api: ApiFixture,
    pg: Running,
    owner: Client,
    root: Client,
}

impl Fixture {
    async fn new() -> Self {
        let api = ApiFixture::new().await;
        api.admin
            .roles()
            .create(NewRole {
                name: "root".into(),
                login: true,
                superuser: true,
                password: Some(Verifier::generate("root-test-pw")),
                ..NewRole::default()
            })
            .unwrap();
        let config = PostgresqlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: TlsMode::Disabled,
            certificate: None,
            private_key: None,
            client_ca: None,
            certificates: BTreeMap::new(),
            databases: vec!["postgres".into()],
            loopback_trust: false,
            connections: 32,
        };
        let listener = config.build().unwrap();
        let pg = serve_postgresql(config.listen, api.service.clone(), Arc::new(listener))
            .await
            .unwrap();
        let (root, _) = Self::connect(&pg, "root").await;
        root.batch_execute("CREATE ROLE owner LOGIN CREATEROLE PASSWORD 'owner-test-pw'")
            .await
            .unwrap();
        root.batch_execute("GRANT CREATE ON SCHEMA public TO owner")
            .await
            .unwrap();
        let (owner, _) = Self::connect(&pg, "owner").await;
        for sql in [
            "CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'original')",
            "CREATE ROLE app LOGIN PASSWORD 'app-test-pw'",
            "CREATE ROLE other LOGIN PASSWORD 'other-test-pw'",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE t TO app",
            "GRANT SELECT, INSERT, UPDATE ON TABLE t TO other",
        ] {
            owner.batch_execute(sql).await.unwrap();
        }
        let mut bindings = api.bindings.clone();
        bindings[0].role = Role::Database { name: "app".into() };
        api.service.reload_bindings(bindings).unwrap();
        Self {
            api,
            pg,
            owner,
            root,
        }
    }

    async fn connect(pg: &Running, role: &str) -> (Client, Closed) {
        let mut config = Config::new();
        config
            .host("localhost")
            .hostaddr(pg.address.ip())
            .port(pg.address.port())
            .user(role)
            .password(format!("{role}-test-pw"))
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect_timeout(Duration::from_secs(10));
        let (client, connection) = config.connect(NoTls).await.unwrap();
        let (send, closed) = oneshot::channel();
        tokio::spawn(async move {
            let _ = send.send(connection.await);
        });
        (client, closed)
    }

    async fn fence(&self) {
        self.owner
            .batch_execute("ALTER ROLE app NOLOGIN")
            .await
            .unwrap();
        self.owner
            .batch_execute("REVOKE INSERT, UPDATE, DELETE ON TABLE t FROM app")
            .await
            .unwrap();
    }

    async fn owner_write(&self, insert: bool) {
        let sql = if insert {
            "INSERT INTO t VALUES (2, 'b')"
        } else {
            "UPDATE t SET v='b' WHERE id=1"
        };
        timeout(PROMPT, self.owner.batch_execute(sql))
            .await
            .expect("idle invalidated session retained its row lock")
            .unwrap();
        let rows = self
            .owner
            .simple_query(if insert {
                "SELECT v FROM t WHERE id=2"
            } else {
                "SELECT v FROM t WHERE id=1"
            })
            .await
            .unwrap();
        let value = rows.iter().find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0),
            _ => None,
        });
        assert_eq!(value, Some("b"));
    }

    async fn stop(self) {
        drop(self.owner);
        drop(self.root);
        self.pg.shutdown().await.unwrap();
        self.api.stop().await;
    }
}

async fn terminated(closed: Closed) {
    // The client sends no request: its connection driver observes the unsolicited
    // FATAL frame (or EOF). A driver without a database error still proves closure.
    let result = timeout(PROMPT, closed)
        .await
        .expect("idle socket was not terminated")
        .unwrap();
    if let Err(error) = result
        && let Some(db) = error.as_db_error()
    {
        assert_eq!(db.severity(), "FATAL");
        assert_eq!(db.code().code(), "57P01");
        assert_eq!(
            db.message(),
            "terminating connection due to administrator command"
        );
    }
}

async fn pg_idle_write(insert: bool) {
    let f = Fixture::new().await;
    let (app, closed) = Fixture::connect(&f.pg, "app").await;
    app.batch_execute(if insert {
        "BEGIN; INSERT INTO t VALUES (2, 'a')"
    } else {
        "BEGIN; UPDATE t SET v='a' WHERE id=1"
    })
    .await
    .unwrap();
    f.fence().await;
    f.owner_write(insert).await;
    terminated(closed).await;
    drop(app);
    f.stop().await;
}

#[tokio::test]
async fn idle_pgwire_update_releases_row_lock() {
    pg_idle_write(false).await;
}
#[tokio::test]
async fn idle_pgwire_insert_releases_key_lock() {
    pg_idle_write(true).await;
}

#[tokio::test]
async fn idle_pgwire_without_transaction_is_terminated() {
    let f = Fixture::new().await;
    let (app, closed) = Fixture::connect(&f.pg, "app").await;
    f.fence().await;
    terminated(closed).await;
    drop(app);
    f.stop().await;
}

#[tokio::test]
async fn unrelated_idle_and_transaction_sessions_survive_changes() {
    let f = Fixture::new().await;
    let (other, _) = Fixture::connect(&f.pg, "other").await;
    let (idle, _) = Fixture::connect(&f.pg, "other").await;
    other
        .batch_execute("BEGIN; INSERT INTO t VALUES (3, 'other')")
        .await
        .unwrap();
    f.fence().await;
    // Password-only generation aliases keep today's bindings valid even
    // though the signal wakes every frontend. LOGIN itself bumps generation,
    // including when it repeats the existing value, so it is not a valid no-op.
    f.owner
        .batch_execute("ALTER ROLE other PASSWORD 'other-new-test-pw'")
        .await
        .unwrap();
    other.batch_execute("COMMIT").await.unwrap();
    idle.batch_execute("SELECT 1").await.unwrap();
    drop((other, idle));
    f.stop().await;
}

#[tokio::test]
async fn dropped_and_recreated_role_does_not_revive_idle_pgwire() {
    let f = Fixture::new().await;
    f.owner
        .batch_execute("CREATE ROLE ephemeral LOGIN PASSWORD 'ephemeral-test-pw'")
        .await
        .unwrap();
    let (app, closed) = Fixture::connect(&f.pg, "ephemeral").await;
    f.owner.batch_execute("DROP ROLE ephemeral").await.unwrap();
    f.owner
        .batch_execute("CREATE ROLE ephemeral LOGIN PASSWORD 'ephemeral-test-pw'")
        .await
        .unwrap();
    terminated(closed).await;
    drop(app);
    f.stop().await;
}

#[tokio::test]
async fn password_change_without_alias_terminates_idle_pgwire() {
    let f = Fixture::new().await;
    let (app, closed) = Fixture::connect(&f.pg, "app").await;
    app.batch_execute("BEGIN; UPDATE t SET v='a' WHERE id=1")
        .await
        .unwrap();
    // A combined attribute/password ALTER does not create a password-only alias.
    f.owner
        .batch_execute("ALTER ROLE app LOGIN PASSWORD 'rotated-test-pw'")
        .await
        .unwrap();
    f.owner_write(false).await;
    terminated(closed).await;
    drop(app);
    f.stop().await;
}

fn native_insert() -> Value {
    json!({"operation":{"request":{"insert":{"table":"t","values":[
        {"column":"id","value":{"literal":{"intValue":"2"}}}
    ]}}}})
}

async fn native_idle_write(grpc: bool, reload: bool, restore: bool) {
    let f = Fixture::new().await;
    let token = f.api.open_http(&f.api.app, false).await;
    let mut client = f.api.grpc_client(&f.api.app).await;
    for request in [json!({"begin":{}}), native_insert()] {
        let bytes = serde_json::to_vec(&request).unwrap();
        if grpc {
            let reply = client
                .execute(rpc(&token, wire_json::decode_execute(&bytes).unwrap()))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(reply.code, "ok");
        } else {
            let (status, body) = f
                .api
                .http(
                    &f.api.app,
                    "POST",
                    "/v1/session/execute",
                    Some(&token),
                    bytes,
                    false,
                )
                .await
                .unwrap();
            assert_eq!(status, 200);
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap()["code"],
                "ok"
            );
        }
    }
    if reload {
        // No executable reload hook: the service signal alone must evict it.
        f.api.service.reload_bindings(vec![]).unwrap();
    } else {
        f.fence().await;
    }
    // Do this before the stale session's next call: that call cannot be what
    // frees the conflicting write, on either certificate-bound transport.
    f.owner_write(true).await;
    if restore {
        f.owner.batch_execute("ALTER ROLE app LOGIN").await.unwrap();
        // A new channel captures the restored role's current lifetime. HTTP
        // requests already create a fresh TLS connection on every call.
        client = f.api.grpc_client(&f.api.app).await;
        let fresh = f.api.open_http(&f.api.app, false).await;
        let (status, _) = f
            .api
            .http(
                &f.api.app,
                "DELETE",
                "/v1/session",
                Some(&fresh),
                vec![],
                false,
            )
            .await
            .unwrap();
        assert_eq!(status, 204);
    }
    let bytes = serde_json::to_vec(&json!({"commit":{}})).unwrap();
    if grpc {
        let status = client
            .execute(rpc(&token, wire_json::decode_execute(&bytes).unwrap()))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    } else {
        let (status, _) = f
            .api
            .http(
                &f.api.app,
                "POST",
                "/v1/session/execute",
                Some(&token),
                bytes,
                false,
            )
            .await
            .unwrap();
        assert_eq!(status, 401);
    }
    drop(client);
    f.stop().await;
}

#[tokio::test]
async fn idle_http_transaction_is_evicted_on_role_fence() {
    native_idle_write(false, false, false).await;
}
#[tokio::test]
async fn idle_grpc_transaction_is_evicted_on_role_fence() {
    native_idle_write(true, false, false).await;
}
#[tokio::test]
async fn idle_http_transaction_is_evicted_on_binding_reload() {
    native_idle_write(false, true, false).await;
}
#[tokio::test]
async fn idle_grpc_transaction_is_evicted_on_binding_reload() {
    native_idle_write(true, true, false).await;
}

#[tokio::test]
async fn restored_login_does_not_revive_evicted_http_transaction() {
    native_idle_write(false, false, true).await;
}

#[tokio::test]
async fn restored_login_does_not_revive_evicted_grpc_transaction() {
    native_idle_write(true, false, true).await;
}
