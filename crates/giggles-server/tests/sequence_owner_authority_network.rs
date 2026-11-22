//! V2 sequence DDL and execution follow the same effective owner.
#![cfg(feature = "engine-rocksdb")]

mod api_support;

use api_support::{Fixture as ApiFixture, rpc};
use giggles_core::{Engine, roles::NewRole};
use giggles_server::{
    Role, Running,
    api::json as wire_json,
    auth::{PostgresqlConfig, serve_postgresql},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Simple,
    Extended,
}

const PROTOCOLS: [Protocol; 2] = [Protocol::Simple, Protocol::Extended];
const ALTER_AND_DROP: [&str; 5] = [
    "ALTER SEQUENCE s OWNED BY NONE",
    "ALTER SEQUENCE s OWNED BY a_t.id",
    "ALTER SEQUENCE s RESTART",
    "ALTER SEQUENCE s INCREMENT BY 5",
    "DROP SEQUENCE s",
];

impl Protocol {
    async fn execute(self, client: &Client, sql: &str) -> Result<(), tokio_postgres::Error> {
        match self {
            Self::Simple => client.simple_query(sql).await.map(|_| ()),
            Self::Extended => client.execute(sql, &[]).await.map(|_| ()),
        }
    }

    async fn ok(self, client: &Client, sql: &str) {
        self.execute(client, sql)
            .await
            .unwrap_or_else(|error| panic!("{self:?}: {sql}: {error}"));
    }

    async fn denied(self, client: &Client, sql: &str) {
        let error = self
            .execute(client, sql)
            .await
            .expect_err(&format!("{self:?}: {sql} succeeded"));
        assert_eq!(
            error.code().map(|code| code.code()),
            Some("42501"),
            "{self:?}: {sql}: {error}"
        );
    }

    async fn next(self, client: &Client) -> i64 {
        let sql = "SELECT nextval('s')";
        match self {
            Self::Simple => {
                for message in client.simple_query(sql).await.unwrap() {
                    if let SimpleQueryMessage::Row(row) = message {
                        return row.get(0).unwrap().parse().unwrap();
                    }
                }
                panic!("{sql}: no row")
            }
            Self::Extended => client.query_one(sql, &[]).await.unwrap().get(0),
        }
    }
}

struct Fixture {
    api: ApiFixture,
    pg: Running,
    root: Client,
    alice: Client,
    bob: Client,
}

impl Fixture {
    async fn new(protocol: Protocol) -> Self {
        let api = ApiFixture::catalog_empty_engine(Engine::V2).await;
        assert_eq!(api.admin.engine(), Engine::V2);
        for name in ["root", "alice", "bob", "carol"] {
            api.admin
                .roles()
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    superuser: name == "root",
                    password: Some(giggles_server::auth::scram::Verifier::generate("secret")),
                    ..Default::default()
                })
                .unwrap();
        }
        let config: PostgresqlConfig = serde_json::from_value(json!({
            "listen": "127.0.0.1:0", "tls": "disabled", "loopback_trust": true
        }))
        .unwrap();
        let pg = serve_postgresql(
            "127.0.0.1:0".parse().unwrap(),
            api.service.clone(),
            Arc::new(config.build().unwrap()),
        )
        .await
        .unwrap();
        let root = Self::connect(&pg, "root").await;
        let alice = Self::connect(&pg, "alice").await;
        let bob = Self::connect(&pg, "bob").await;
        protocol
            .ok(&root, "GRANT CREATE ON SCHEMA public TO alice, bob")
            .await;
        protocol.ok(&root, "GRANT bob TO carol").await;
        protocol
            .ok(&alice, "CREATE TABLE a_t (id BIGINT PRIMARY KEY)")
            .await;
        protocol.ok(&alice, "CREATE SEQUENCE s").await;
        protocol
            .ok(&bob, "CREATE TABLE b_t (id BIGINT PRIMARY KEY)")
            .await;
        Self {
            api,
            pg,
            root,
            alice,
            bob,
        }
    }

    async fn connect(pg: &Running, role: &str) -> Client {
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(pg.address.port())
            .user(role)
            .password("secret")
            .dbname("postgres")
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    async fn link(&self, protocol: Protocol) {
        protocol
            .ok(&self.root, "ALTER SEQUENCE s OWNED BY b_t.id")
            .await;
    }

    async fn stop(self) {
        drop((self.root, self.alice, self.bob));
        self.pg.shutdown().await.unwrap();
        self.api.stop().await;
    }
}

#[tokio::test]
async fn former_owner_cannot_alter_or_drop_a_linked_sequence() {
    for protocol in PROTOCOLS {
        let f = Fixture::new(protocol).await;
        f.link(protocol).await;
        assert_eq!(protocol.next(&f.bob).await, 1);
        for sql in ALTER_AND_DROP {
            protocol.denied(&f.alice, sql).await;
        }

        let owners = f.api.admin.sql_session().sequence_owners().unwrap();
        assert!(
            owners.contains(&("s".into(), Some("b_t".into()))),
            "{owners:?}"
        );
        assert_eq!(protocol.next(&f.bob).await, 2);
        f.stop().await;
    }
}

#[tokio::test]
async fn owner_and_member_can_restart_and_unlink_without_returning_ownership() {
    for protocol in PROTOCOLS {
        let f = Fixture::new(protocol).await;
        let carol = Fixture::connect(&f.pg, "carol").await;
        f.link(protocol).await;
        assert_eq!(
            f.api.admin.roles().table_owner("s").unwrap().as_deref(),
            Some("bob")
        );
        protocol
            .ok(&f.bob, "ALTER SEQUENCE s RESTART WITH 20")
            .await;
        assert_eq!(protocol.next(&f.bob).await, 20);
        protocol
            .ok(&carol, "ALTER SEQUENCE s RESTART WITH 30")
            .await;
        protocol.ok(&carol, "ALTER SEQUENCE s OWNED BY NONE").await;
        assert_eq!(protocol.next(&f.bob).await, 30);
        f.link(protocol).await;
        protocol.ok(&f.bob, "ALTER SEQUENCE s OWNED BY NONE").await;
        assert_eq!(protocol.next(&f.bob).await, 31);
        for sql in [
            "SELECT nextval('s')",
            "ALTER SEQUENCE s RESTART",
            "DROP SEQUENCE s",
        ] {
            protocol.denied(&f.alice, sql).await;
        }
        protocol.ok(&f.bob, "DROP SEQUENCE s").await;
        drop(carol);
        f.stop().await;
    }
}

#[tokio::test]
async fn transactional_link_publishes_and_uses_the_effective_owner() {
    for protocol in PROTOCOLS {
        let f = Fixture::new(protocol).await;
        protocol.ok(&f.root, "BEGIN").await;
        f.link(protocol).await;
        protocol.ok(&f.root, "COMMIT").await;
        assert_eq!(
            f.api.admin.roles().table_owner("s").unwrap().as_deref(),
            Some("bob")
        );
        for sql in ALTER_AND_DROP {
            protocol.ok(&f.alice, "BEGIN").await;
            protocol.denied(&f.alice, sql).await;
            protocol.ok(&f.alice, "ROLLBACK").await;
        }
        // Simulate an older database whose sequence record still names its creator.
        f.api.admin.roles().set_table_owner("s", "alice").unwrap();
        protocol.ok(&f.bob, "BEGIN").await;
        protocol
            .ok(&f.bob, "ALTER TABLE b_t RENAME TO renamed_b_t")
            .await;
        protocol.ok(&f.bob, "ALTER SEQUENCE s OWNED BY NONE").await;
        assert_eq!(protocol.next(&f.bob).await, 1);
        protocol.ok(&f.bob, "COMMIT").await;
        assert_eq!(protocol.next(&f.bob).await, 2);
        protocol.denied(&f.alice, "ALTER SEQUENCE s RESTART").await;
        f.stop().await;
    }
}

#[tokio::test]
async fn repeatable_read_cannot_authorize_ddl_using_the_old_link() {
    for protocol in PROTOCOLS {
        let f = Fixture::new(protocol).await;
        protocol
            .ok(&f.alice, "BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await;
        assert_eq!(protocol.next(&f.alice).await, 1);
        f.link(protocol).await;
        // Force the test to distinguish the live link from the sequence record.
        f.api.admin.roles().set_table_owner("s", "alice").unwrap();
        protocol.denied(&f.alice, "ALTER SEQUENCE s RESTART").await;
        protocol.ok(&f.alice, "ROLLBACK").await;
        assert_eq!(protocol.next(&f.bob).await, 2);
        f.stop().await;
    }
}

#[tokio::test]
async fn create_owned_by_records_the_table_owner_in_both_modes() {
    for protocol in PROTOCOLS {
        let f = Fixture::new(protocol).await;
        protocol
            .ok(&f.root, "CREATE SEQUENCE linked_auto OWNED BY b_t.id")
            .await;
        protocol.ok(&f.root, "BEGIN").await;
        protocol
            .ok(&f.root, "CREATE SEQUENCE linked_tx OWNED BY b_t.id")
            .await;
        protocol.ok(&f.root, "COMMIT").await;
        for name in ["linked_auto", "linked_tx"] {
            assert_eq!(
                f.api.admin.roles().table_owner(name).unwrap().as_deref(),
                Some("bob"),
                "{name}"
            );
            protocol.ok(&f.bob, &format!("DROP SEQUENCE {name}")).await;
        }
        f.stop().await;
    }
}

#[tokio::test]
async fn standalone_sequence_ddl_still_belongs_to_its_creator() {
    for protocol in PROTOCOLS {
        let f = Fixture::new(protocol).await;
        protocol.denied(&f.bob, "ALTER SEQUENCE s RESTART").await;
        protocol.denied(&f.bob, "DROP SEQUENCE s").await;
        protocol
            .ok(&f.alice, "ALTER SEQUENCE s RESTART WITH 50")
            .await;
        assert_eq!(protocol.next(&f.alice).await, 50);
        protocol.ok(&f.alice, "DROP SEQUENCE s").await;
        f.stop().await;
    }
}

#[tokio::test]
async fn multi_name_drop_is_atomic_when_a_linked_sequence_is_denied() {
    for protocol in PROTOCOLS {
        let f = Fixture::new(protocol).await;
        f.link(protocol).await;
        protocol.ok(&f.alice, "CREATE SEQUENCE mine").await;
        protocol.denied(&f.alice, "DROP SEQUENCE mine, s").await;

        let owners = f.api.admin.sql_session().sequence_owners().unwrap();
        assert!(owners.contains(&("mine".into(), None)), "{owners:?}");
        assert!(
            owners.contains(&("s".into(), Some("b_t".into()))),
            "{owners:?}"
        );
        f.stop().await;
    }
}

#[tokio::test]
async fn sequence_ddl_uses_the_resolved_schema_and_owner() {
    for protocol in PROTOCOLS {
        let f = Fixture::new(protocol).await;
        f.link(protocol).await;
        protocol.ok(&f.root, "CREATE SCHEMA alice_schema").await;
        protocol
            .ok(
                &f.root,
                "GRANT USAGE, CREATE ON SCHEMA alice_schema TO alice, bob",
            )
            .await;
        protocol
            .ok(
                &f.bob,
                "CREATE TABLE alice_schema.b_t (id BIGINT PRIMARY KEY)",
            )
            .await;
        protocol
            .ok(&f.alice, "CREATE SEQUENCE alice_schema.linked")
            .await;
        protocol
            .ok(
                &f.root,
                "ALTER SEQUENCE alice_schema.linked OWNED BY alice_schema.b_t.id",
            )
            .await;
        protocol
            .denied(&f.alice, "ALTER SEQUENCE alice_schema.linked RESTART")
            .await;
        protocol
            .denied(&f.alice, "DROP SEQUENCE alice_schema.linked")
            .await;
        protocol
            .ok(&f.bob, "ALTER SEQUENCE alice_schema.linked RESTART")
            .await;

        protocol
            .ok(&f.alice, "CREATE SEQUENCE alice_schema.s")
            .await;
        protocol
            .ok(&f.alice, "SET search_path TO alice_schema, public")
            .await;
        protocol
            .ok(&f.alice, "ALTER SEQUENCE s RESTART WITH 30")
            .await;
        assert_eq!(protocol.next(&f.alice).await, 30);
        protocol
            .denied(&f.alice, "ALTER SEQUENCE public.s RESTART")
            .await;
        protocol.denied(&f.alice, "DROP SEQUENCE public.s").await;
        protocol.ok(&f.alice, "DROP SEQUENCE s").await;
        assert_eq!(protocol.next(&f.bob).await, 1);
        f.stop().await;
    }
}

#[tokio::test]
async fn prepared_restart_is_denied_if_the_sequence_is_linked_before_bind() {
    let protocol = Protocol::Extended;
    let f = Fixture::new(protocol).await;
    assert_eq!(protocol.next(&f.alice).await, 1);
    let restart = f.alice.prepare("ALTER SEQUENCE s RESTART").await.unwrap();
    f.link(protocol).await;

    let error = f.alice.execute(&restart, &[]).await.unwrap_err();
    assert_eq!(
        error.code().map(|code| code.code()),
        Some("42501"),
        "{error}"
    );
    assert_eq!(protocol.next(&f.bob).await, 2);
    f.stop().await;
}

async fn native_database_token(f: &ApiFixture, role: &str) -> String {
    let mut bindings = f.bindings.clone();
    bindings[0].role = Role::Database { name: role.into() };
    f.service.reload_bindings(bindings).unwrap();
    f.open_http(&f.app, false).await
}

async fn native_sequence(f: &ApiFixture, token: &str, command: &str, grpc: bool) -> Value {
    let request = serde_json::to_vec(&json!({"catalog": {command: {"name": "s"}}})).unwrap();
    if grpc {
        let wire = wire_json::decode_execute(&request).unwrap();
        let reply = f
            .grpc_client(&f.app)
            .await
            .execute(rpc(token, wire))
            .await
            .unwrap()
            .into_inner();
        serde_json::from_slice(&wire_json::encode_reply(&reply).unwrap()).unwrap()
    } else {
        let (status, bytes) = f
            .http(
                &f.app,
                "POST",
                "/v1/session/execute",
                Some(token),
                request,
                false,
            )
            .await
            .unwrap();
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&bytes));
        serde_json::from_slice(&bytes).unwrap()
    }
}

#[tokio::test]
async fn native_drop_sequence_checks_the_effective_owner() {
    for grpc in [false, true] {
        let protocol = Protocol::Extended;
        let f = Fixture::new(protocol).await;
        f.link(protocol).await;
        f.api.admin.roles().set_table_owner("s", "alice").unwrap();
        let token = native_database_token(&f.api, "alice").await;
        let reply = native_sequence(&f.api, &token, "dropSequence", grpc).await;
        assert_eq!(reply["code"], "42501", "grpc={grpc}: {reply}");
        assert_eq!(protocol.next(&f.bob).await, 1);
        f.stop().await;
    }
}

#[tokio::test]
async fn native_drop_sequence_allows_the_effective_owner_and_its_member() {
    for grpc in [false, true] {
        for role in ["bob", "carol"] {
            let protocol = Protocol::Extended;
            let f = Fixture::new(protocol).await;
            f.link(protocol).await;
            f.api.admin.roles().set_table_owner("s", "alice").unwrap();
            let token = native_database_token(&f.api, role).await;
            let reply = native_sequence(&f.api, &token, "dropSequence", grpc).await;
            assert_eq!(reply["code"], "ok", "{role}, grpc={grpc}: {reply}");
            assert!(!f.api.admin.sql_session().relation_exists("s").unwrap());
            f.stop().await;
        }
    }
}

#[tokio::test]
async fn legacy_native_sequence_creator_owns_and_can_drop_it() {
    use giggles_core::roles::SchemaPrivilege;

    for grpc in [false, true] {
        let f = ApiFixture::catalog_empty_engine(Engine::Legacy).await;
        assert_eq!(f.admin.engine(), Engine::Legacy);
        let roles = f.admin.roles();
        roles
            .create(NewRole {
                name: "alice".into(),
                login: true,
                ..Default::default()
            })
            .unwrap();
        roles
            .grant_schema(
                "public",
                "alice",
                &[SchemaPrivilege::Create, SchemaPrivilege::Usage],
            )
            .unwrap();
        let token = native_database_token(&f, "alice").await;

        let created = native_sequence(&f, &token, "createSequence", grpc).await;
        assert_eq!(created["code"], "ok", "grpc={grpc}: {created}");
        assert_eq!(roles.table_owner("s").unwrap().as_deref(), Some("alice"));
        let dropped = native_sequence(&f, &token, "dropSequence", grpc).await;
        assert_eq!(dropped["code"], "ok", "grpc={grpc}: {dropped}");
        assert!(!f.admin.sql_session().relation_exists("s").unwrap());
        f.stop().await;
    }
}
