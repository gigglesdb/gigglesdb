//! GD-0077 E3-1: real frontend conformance; deliberately retain fence failures.
mod api_support;

use api_support::{Fixture as ApiFixture, rpc};
use giggles_core::roles::NewRole;
use giggles_server::{
    Role, Running,
    api::{json as wire_json, proto},
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, config::SslMode};

struct Fixture {
    api: ApiFixture,
    pg: Running,
    owner: Client,
    root: Client,
}

fn pg_result<T>(result: &Result<T, tokio_postgres::Error>) -> String {
    match result {
        Ok(_) => "ok".into(),
        Err(error) => format!(
            "SQLSTATE={} closed={} {error}",
            error.code().map_or("none", |code| code.code()),
            error.is_closed()
        ),
    }
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
        let root = Self::connect_at(&pg, "root", "root-test-pw").await.unwrap();
        root.batch_execute("CREATE ROLE owner LOGIN CREATEROLE PASSWORD 'owner-test-pw'")
            .await
            .unwrap();
        root.batch_execute("GRANT CREATE ON SCHEMA public TO owner")
            .await
            .unwrap();
        let owner = Self::connect_at(&pg, "owner", "owner-test-pw")
            .await
            .unwrap();
        owner
            .batch_execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
            .await
            .unwrap();
        owner
            .batch_execute("CREATE ROLE app LOGIN PASSWORD 'app-test-pw'")
            .await
            .unwrap();
        owner
            .batch_execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE t TO app")
            .await
            .unwrap();
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

    async fn connect_at(
        pg: &Running,
        role: &str,
        password: &str,
    ) -> Result<Client, tokio_postgres::Error> {
        let mut config = Config::new();
        config
            .host("localhost")
            .hostaddr(pg.address.ip())
            .port(pg.address.port())
            .user(role)
            .password(password)
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect_timeout(Duration::from_secs(10));
        let (client, connection) = config.connect(NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    async fn app(&self) -> Client {
        Self::connect_at(&self.pg, "app", "app-test-pw")
            .await
            .unwrap()
    }

    async fn fence(&self) {
        self.owner
            .batch_execute("ALTER ROLE app NOLOGIN")
            .await
            .unwrap();
        let sql = "REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLE t FROM app";
        let result = self.owner.batch_execute(sql).await;
        println!("fence grammar: {sql}: {}", pg_result(&result));
        if result.is_err() {
            // Keep testing the accepted write privileges when TRUNCATE is unsupported.
            self.owner
                .batch_execute("REVOKE INSERT, UPDATE, DELETE ON TABLE t FROM app")
                .await
                .unwrap();
        }
    }

    async fn rows(&self) -> usize {
        self.owner
            .simple_query("SELECT id FROM t ORDER BY id")
            .await
            .unwrap()
            .into_iter()
            .filter(|message| matches!(message, SimpleQueryMessage::Row(_)))
            .count()
    }

    async fn stop(self) {
        drop(self.owner);
        drop(self.root);
        self.pg.shutdown().await.unwrap();
        self.api.stop().await;
    }
}

#[tokio::test]
async fn s1_pgwire_new_login_is_refused() {
    let f = Fixture::new().await;
    f.fence().await;
    let result = Fixture::connect_at(&f.pg, "app", "app-test-pw").await;
    let refused = result.as_ref().is_err_and(|error| {
        error
            .code()
            .is_some_and(|code| code.code().starts_with("28"))
    });
    println!("S1 new login: {}", pg_result(&result));
    drop(result);
    f.stop().await;
    // Fence guarantee (GD-0077 E3): a fenced role must not authenticate a new pgwire connection.
    assert!(
        refused,
        "NOLOGIN did not reject authentication with class 28"
    );
}

async fn pgwire_transaction(restore_login: bool) {
    let f = Fixture::new().await;
    let app = f.app().await;
    app.batch_execute("BEGIN").await.unwrap();
    app.batch_execute("INSERT INTO t VALUES (1)").await.unwrap();
    assert_eq!(
        f.rows().await,
        0,
        "pre-fence write must still be uncommitted"
    );
    f.fence().await;
    if restore_login {
        f.owner.batch_execute("ALTER ROLE app LOGIN").await.unwrap();
    }
    let result = app.batch_execute("COMMIT").await;
    let rows = f.rows().await;
    println!(
        "S2 restore_login={restore_login}: COMMIT={} rows={rows}",
        pg_result(&result)
    );
    let refused = result.is_err();
    drop(app);
    f.stop().await;
    // Fence guarantee (GD-0077 E3): NOLOGIN must fence an already staged transaction, even after LOGIN.
    assert!(
        refused && rows == 0,
        "fenced COMMIT refused={refused}, durable rows={rows}"
    );
}

#[tokio::test]
async fn s2_pgwire_open_transaction_cannot_commit() {
    pgwire_transaction(false).await;
}

#[tokio::test]
async fn s2_pgwire_login_restoration_does_not_revive_transaction() {
    pgwire_transaction(true).await;
}

#[tokio::test]
async fn s3_pgwire_old_session_cannot_autocommit() {
    let f = Fixture::new().await;
    let app = f.app().await;
    app.batch_execute("SELECT 1").await.unwrap();
    f.fence().await;
    let result = app.batch_execute("INSERT INTO t VALUES (1)").await;
    let rows = f.rows().await;
    println!("S3 INSERT={} rows={rows}", pg_result(&result));
    let refused = result.is_err();
    drop(app);
    f.stop().await;
    // Fence guarantee (GD-0077 E3): an authenticated pre-fence session must lose autocommit write access.
    assert!(
        refused && rows == 0,
        "fenced INSERT refused={refused}, durable rows={rows}"
    );
}

struct Reply {
    ok: bool,
    refused: bool,
    detail: String,
}

enum Native {
    Http,
    Grpc(Box<proto::giggles_client::GigglesClient<tonic::transport::Channel>>),
}

impl Native {
    async fn execute(&mut self, f: &Fixture, token: &str, request: Value) -> Reply {
        let bytes = serde_json::to_vec(&request).unwrap();
        match self {
            Self::Http => {
                let (status, body) = f
                    .api
                    .http(
                        &f.api.app,
                        "POST",
                        "/v1/session/execute",
                        Some(token),
                        bytes,
                        false,
                    )
                    .await
                    .unwrap();
                let body: Value = serde_json::from_slice(&body).unwrap();
                let ok = status == 200 && body["code"] == "ok";
                let refused = matches!(status, 401 | 403)
                    || matches!(body["code"].as_str(), Some("28000" | "28P01" | "42501"));
                Reply {
                    ok,
                    refused,
                    detail: format!("HTTP {status} {body}"),
                }
            }
            Self::Grpc(client) => {
                let wire = wire_json::decode_execute(&bytes).unwrap();
                match client.execute(rpc(token, wire)).await {
                    Ok(response) => {
                        let response = response.into_inner();
                        let detail =
                            String::from_utf8(wire_json::encode_reply(&response).unwrap()).unwrap();
                        Reply {
                            ok: response.code == "ok",
                            refused: matches!(response.code.as_str(), "28000" | "28P01" | "42501"),
                            detail,
                        }
                    }
                    Err(status) => Reply {
                        ok: false,
                        refused: matches!(
                            status.code(),
                            tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
                        ),
                        detail: format!("gRPC {:?}: {}", status.code(), status.message()),
                    },
                }
            }
        }
    }
}

fn native_insert() -> Value {
    json!({"operation":{"request":{"insert":{"table":"t","values":[
        {"column":"id","value":{"literal":{"intValue":"1"}}}
    ]}}}})
}

async fn native_fence(grpc: bool, transaction: bool, restore_login: bool) {
    let f = Fixture::new().await;
    let token = f.api.open_http(&f.api.app, false).await;
    // Retain the same gRPC channel, as well as the same logical session across calls.
    let mut client = if grpc {
        Native::Grpc(Box::new(f.api.grpc_client(&f.api.app).await))
    } else {
        Native::Http
    };
    if transaction {
        let begin = client.execute(&f, &token, json!({"begin":{}})).await;
        assert!(begin.ok, "typed BEGIN: {}", begin.detail);
        let write = client.execute(&f, &token, native_insert()).await;
        assert!(write.ok, "pre-fence native write: {}", write.detail);
        assert_eq!(f.rows().await, 0, "native write must remain uncommitted");
    }
    f.fence().await;
    if restore_login {
        f.owner.batch_execute("ALTER ROLE app LOGIN").await.unwrap();
    }
    let request = if transaction {
        json!({"commit":{}})
    } else {
        native_insert()
    };
    let reply = client.execute(&f, &token, request).await;
    let rows = f.rows().await;
    println!(
        "S4 grpc={grpc} transaction={transaction} restore_login={restore_login}: {} rows={rows}",
        reply.detail
    );
    drop(client);
    f.stop().await;
    // Fence guarantee (GD-0077 E3): certificate-bound SQL roles must lose native write/COMMIT authority.
    assert!(
        reply.refused && rows == 0,
        "native fence: {}; durable rows={rows}",
        reply.detail
    );
}

#[tokio::test]
async fn s4_http_old_session_cannot_write() {
    native_fence(false, false, false).await;
}
#[tokio::test]
async fn s4_grpc_old_session_cannot_write() {
    native_fence(true, false, false).await;
}
#[tokio::test]
async fn s4_http_open_transaction_cannot_commit() {
    native_fence(false, true, false).await;
}
#[tokio::test]
async fn s4_grpc_open_transaction_cannot_commit() {
    native_fence(true, true, false).await;
}
#[tokio::test]
async fn s4_http_login_restoration_does_not_revive_transaction() {
    native_fence(false, true, true).await;
}
#[tokio::test]
async fn s4_grpc_login_restoration_does_not_revive_transaction() {
    native_fence(true, true, true).await;
}

#[tokio::test]
async fn s5_membership_capability_is_recorded() {
    let f = Fixture::new().await;
    f.owner
        .batch_execute("CREATE ROLE u LOGIN PASSWORD 'u-test-pw'")
        .await
        .unwrap();
    let grant = f.owner.batch_execute("GRANT app TO u").await;
    println!("S5 GRANT app TO u: {}", pg_result(&grant));
    let members = f
        .owner
        .simple_query("SELECT roleid, member FROM pg_catalog.pg_auth_members")
        .await;
    println!("S5 pg_auth_members: {}", pg_result(&members));
    if let Ok(rows) = &members {
        println!(
            "S5 membership rows={}",
            rows.iter()
                .filter(|r| matches!(r, SimpleQueryMessage::Row(_)))
                .count()
        );
    }
    if grant.is_err() {
        println!(
            "S5 RECORDED: role membership unavailable; SET ROLE and inherited-write fence probes skipped"
        );
        f.stop().await;
        return;
    }
    let u = Fixture::connect_at(&f.pg, "u", "u-test-pw").await.unwrap();
    let set_role = u.batch_execute("SET ROLE app").await;
    println!("S5 SET ROLE app: {}", pg_result(&set_role));
    // Membership semantics are observational until separately qualified against PG18.
    u.batch_execute("BEGIN").await.unwrap();
    let write = u.batch_execute("INSERT INTO t VALUES (1)").await;
    println!("S5 pre-fence member INSERT: {}", pg_result(&write));
    if write.is_ok() {
        f.fence().await;
        let commit = u.batch_execute("COMMIT").await;
        println!(
            "S5 member COMMIT: {}; rows={}",
            pg_result(&commit),
            f.rows().await
        );
        let write = u.batch_execute("INSERT INTO t VALUES (2)").await;
        println!(
            "S5 member post-fence INSERT: {}; rows={}",
            pg_result(&write),
            f.rows().await
        );
    } else {
        let _ = u.batch_execute("ROLLBACK").await;
        println!("S5 RECORDED: no usable SET ROLE/inherited write path; remaining probes skipped");
    }
    drop(u);
    f.stop().await;
}

#[tokio::test]
async fn s6_owner_and_superuser_residual_writes_are_recorded() {
    let f = Fixture::new().await;
    f.fence().await;
    // owner is a non-superuser with CREATEROLE; root is a distinct superuser.
    let owner = f.owner.batch_execute("INSERT INTO t VALUES (1)").await;
    let root = f.root.batch_execute("INSERT INTO t VALUES (2)").await;
    println!("S6 non-superuser table owner INSERT: {}", pg_result(&owner));
    println!(
        "S6 superuser INSERT: {}; rows={}",
        pg_result(&root),
        f.rows().await
    );
    f.stop().await;
}

#[tokio::test]
async fn s7_session_inventory_and_termination_are_recorded() {
    let f = Fixture::new().await;
    let app = f.app().await;
    let pid = app
        .simple_query("SELECT pg_backend_pid()")
        .await
        .unwrap()
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_owned()),
            _ => None,
        })
        .unwrap();
    for sql in [
        "SELECT pid, usename FROM pg_stat_activity WHERE usename = 'app'".to_owned(),
        format!("SELECT pg_terminate_backend({pid})"),
        "SELECT pid::text, pg_terminate_backend(pid)::text FROM pg_stat_activity WHERE datname=current_database() AND usename IN ('app') AND pid<>pg_backend_pid()".to_owned(),
    ] {
        let result = f.owner.simple_query(&sql).await;
        println!("S7 {sql}: {}", pg_result(&result));
        if let Ok(messages) = result {
            println!("S7 result: {messages:?}");
        }
    }
    let probe = app.batch_execute("SELECT 1").await;
    println!(
        "S7 app connection after termination probes: {}",
        pg_result(&probe)
    );
    drop(app);
    f.stop().await;
}

#[tokio::test]
async fn fence_privilege_and_cascade_grammar_is_recorded() {
    let f = Fixture::new().await;
    for sql in [
        "GRANT TRUNCATE ON TABLE t TO app",
        "REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLE t FROM app",
        "REVOKE INSERT, UPDATE, DELETE ON TABLE t FROM app CASCADE",
    ] {
        let result = f.owner.batch_execute(sql).await;
        println!("GRAMMAR {sql}: {}", pg_result(&result));
    }
    f.stop().await;
}
