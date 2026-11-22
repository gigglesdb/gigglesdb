//! Catalog changes must invalidate the sequence grants for a named INSERT.
#![cfg(feature = "engine-rocksdb")]

mod api_support;
use api_support::Fixture as ApiFixture;
use giggles_core::{Engine, roles::NewRole};
use giggles_server::{
    Running,
    auth::{PostgresqlConfig, serve_postgresql},
};
use std::sync::Arc;
use tokio_postgres::{Client, NoTls, Statement};

struct Fixture {
    api: ApiFixture,
    pg: Running,
    root: Client,
    member: Client,
}

impl Fixture {
    async fn new() -> Self {
        let api = ApiFixture::catalog_empty_engine(Engine::V2).await;
        for (name, superuser) in [("root", true), ("owner", false), ("member", false), ("other", false)] {
            api.admin.roles().create(NewRole {
                name: name.into(), login: true, superuser, ..Default::default()
            }).unwrap();
        }
        let config: PostgresqlConfig = serde_json::from_value(serde_json::json!({
            "listen": "127.0.0.1:0", "tls": "disabled", "loopback_trust": true
        })).unwrap();
        let pg = serve_postgresql(
            "127.0.0.1:0".parse().unwrap(), api.service.clone(), Arc::new(config.build().unwrap()),
        ).await.unwrap();
        let root = connect(&pg, "root").await;
        let member = connect(&pg, "member").await;
        run(&root, "GRANT CREATE ON SCHEMA public TO owner, other").await;
        let owner = connect(&pg, "owner").await;
        run(&owner, "CREATE TABLE memo_t (id BIGSERIAL PRIMARY KEY, v BIGINT)").await;
        run(&owner, "GRANT INSERT, SELECT ON memo_t TO member").await;
        let other = connect(&pg, "other").await;
        run(&other, "CREATE TABLE other_t (id BIGINT PRIMARY KEY)").await;
        run(&root, "GRANT owner TO member").await;
        Self { api, pg, root, member }
    }

    fn catalog_version(&self) -> u64 {
        let sql = self.api.admin.sql_session();
        sql.authorization_catalog_key(&self.api.admin).unwrap().0
    }

    async fn publish(&self, ddl: &str) {
        let before = self.catalog_version();
        run(&self.root, ddl).await;
        assert_ne!(self.catalog_version(), before, "{ddl} must invalidate the S2 catalog key");
    }

    async fn warm_insert(&self) -> Statement {
        let insert = self.member.prepare("INSERT INTO memo_t (v) VALUES ($1) RETURNING id").await.unwrap();
        let row = self.member.query_one(&insert, &[&10_i64]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), 1);
        insert
    }

    async fn assert_refused(&self, insert: &Statement) {
        let error = self.member.query_one(insert, &[&20_i64]).await.unwrap_err();
        assert_eq!(error.code().map(|code| code.code()), Some("42501"));
    }

    async fn assert_next(&self, expected: i64) {
        let row = self.root.query_one("SELECT nextval('memo_t_id_seq')", &[]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), expected);
    }

    async fn stop(self) {
        drop(self.member);
        drop(self.root);
        self.pg.shutdown().await.unwrap();
        self.api.stop().await;
    }
}

async fn connect(pg: &Running, role: &str) -> Client {
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1").port(pg.address.port()).user(role).dbname("postgres")
        .connect(NoTls).await.unwrap();
    tokio::spawn(async move { let _ = connection.await; });
    client
}

async fn run(client: &Client, sql: &str) {
    client.batch_execute(sql).await.unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}

#[tokio::test]
async fn named_insert_rechecks_membership_after_revoke() {
    let f = Fixture::new().await;
    let insert = f.warm_insert().await;
    f.publish("REVOKE owner FROM member").await;
    f.assert_refused(&insert).await;
    f.assert_next(2).await;
    f.stop().await;
}

#[tokio::test]
async fn named_insert_rechecks_membership_in_repeatable_read() {
    let f = Fixture::new().await;
    run(&f.member, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    let insert = f.warm_insert().await;
    f.publish("REVOKE owner FROM member").await;
    f.assert_refused(&insert).await;
    run(&f.member, "ROLLBACK").await;
    f.assert_next(2).await;
    f.stop().await;
}

#[tokio::test]
async fn named_insert_rechecks_a_transferred_sequence() {
    let f = Fixture::new().await;
    let insert = f.warm_insert().await;
    f.publish("ALTER SEQUENCE memo_t_id_seq OWNED BY other_t.id").await;
    f.assert_refused(&insert).await;
    f.assert_next(2).await;
    f.stop().await;
}

#[tokio::test]
async fn named_insert_rechecks_a_recreated_sequence() {
    let f = Fixture::new().await;
    let insert = f.warm_insert().await;
    f.publish("DROP SEQUENCE memo_t_id_seq CASCADE").await;
    f.publish("CREATE SEQUENCE memo_t_id_seq START 100 OWNED BY other_t.id").await;
    f.publish("ALTER TABLE memo_t ALTER COLUMN id SET DEFAULT nextval('memo_t_id_seq')").await;
    f.assert_refused(&insert).await;
    f.assert_next(100).await;
    f.stop().await;
}
