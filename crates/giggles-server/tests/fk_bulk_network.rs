#![cfg(feature = "engine-rocksdb")]

use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};

const COUNT_USERS: &str = r#"SELECT count(*) FROM "User""#;
const COUNT_POSTS: &str = r#"SELECT count(*) FROM "Post""#;
const COUNT_ORPHANS: &str = r#"SELECT count(*) FROM "Post" p
    WHERE NOT EXISTS (SELECT 1 FROM "User" u WHERE u."id" = p."authorId")"#;

async fn count(client: &tokio_postgres::Client, sql: &str, expected: i64) {
    let actual: i64 = client.query_one(sql, &[]).await.unwrap().get(0);
    assert_eq!(actual, expected, "{sql}");
}

#[tokio::test]
async fn writable_cte_queue_claim_skips_the_other_sessions_lock() {
    let path = std::env::temp_dir().join(format!("fk-queue-claim-{}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "postgres".into(),
            login: true,
            superuser: true,
            ..Default::default()
        })
        .unwrap();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: BTreeMap::new(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 8,
    };
    let running = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let mut client_config = tokio_postgres::Config::new();
    client_config
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("postgres")
        .dbname("postgres");
    let (first, connection) = client_config.connect(tokio_postgres::NoTls).await.unwrap();
    let first_task = tokio::spawn(connection);
    let (second, connection) = client_config.connect(tokio_postgres::NoTls).await.unwrap();
    let second_task = tokio::spawn(connection);

    first
        .batch_execute(
            "CREATE TABLE jobs(id int PRIMARY KEY, label text NOT NULL); \
        INSERT INTO jobs VALUES (1, 'ready'), (2, 'ready')",
        )
        .await
        .unwrap();
    first.batch_execute("BEGIN").await.unwrap();
    second.batch_execute("BEGIN").await.unwrap();
    let claim = "WITH next_job AS (SELECT id FROM jobs WHERE label='ready' ORDER BY id \
        FOR UPDATE SKIP LOCKED LIMIT 1), claimed AS (UPDATE jobs SET label='running' FROM next_job \
        WHERE jobs.id=next_job.id RETURNING jobs.id,jobs.label) SELECT * FROM claimed ORDER BY id";
    let first_job = first.query_one(claim, &[]).await.unwrap();
    let second_job = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        second.query_one(claim, &[]),
    )
    .await
    .expect("second claim must finish while the first still holds its row lock")
    .unwrap();
    assert_eq!(first_job.get::<_, i32>(0), 1);
    assert_eq!(second_job.get::<_, i32>(0), 2);
    assert_eq!(first_job.get::<_, &str>(1), "running");
    assert_eq!(second_job.get::<_, &str>(1), "running");
    second.batch_execute("COMMIT").await.unwrap();
    first.batch_execute("COMMIT").await.unwrap();
    count(&first, "SELECT count(*) FROM jobs WHERE label='running'", 2).await;

    drop((first, second));
    first_task.await.unwrap().unwrap();
    second_task.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn prisma_create_many_and_delete_many() {
    let path = std::env::temp_dir().join(format!("fk-bulk-network-{}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "postgres".into(),
            login: true,
            superuser: true,
            ..Default::default()
        })
        .unwrap();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: BTreeMap::new(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 8,
    };
    let running = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("postgres")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let task = tokio::spawn(connection);

    client
        .batch_execute(
            r#"
        CREATE TABLE "User" ("id" text PRIMARY KEY);
        CREATE TABLE "Post" (
            "id" text PRIMARY KEY,
            "authorId" text NOT NULL,
            "published" boolean NOT NULL,
            CONSTRAINT "Post_authorId_fkey" FOREIGN KEY ("authorId") REFERENCES "User"("id")
                ON DELETE RESTRICT ON UPDATE CASCADE
        );
        CREATE INDEX "Post_authorId_idx" ON "Post"("authorId");
    "#,
        )
        .await
        .unwrap();

    for first in (0..5_000).step_by(250) {
        let values = (first..first + 250)
            .map(|i| format!("('user-{i:05}')"))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            client
                .execute(
                    &format!(r#"INSERT INTO "User" ("id") VALUES {values}"#),
                    &[]
                )
                .await
                .unwrap(),
            250
        );
    }
    for first in (0..20_000).step_by(250) {
        let values = (first..first + 250)
            .map(|i| format!("('post-{i:05}', 'user-{:05}', false)", i % 5_000))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            client
                .execute(
                    &format!(
                        r#"INSERT INTO "Post" ("id", "authorId", "published") VALUES {values}"#
                    ),
                    &[]
                )
                .await
                .unwrap(),
            250
        );
    }
    count(&client, COUNT_ORPHANS, 0).await;

    let error = client
        .execute(r#"DELETE FROM "User""#, &[])
        .await
        .unwrap_err();
    let diagnostic = error.as_db_error().unwrap();
    assert_eq!(diagnostic.code().code(), "23001");
    assert_eq!(
        diagnostic.message(),
        r#"update or delete on table "User" violates RESTRICT setting of foreign key constraint "Post_authorId_fkey" on table "Post""#
    );
    assert_eq!(
        diagnostic.detail(),
        Some(r#"Key (id)=(user-00000) is referenced from table "Post"."#)
    );
    count(&client, COUNT_USERS, 5_000).await;
    count(&client, COUNT_POSTS, 20_000).await;
    count(&client, COUNT_ORPHANS, 0).await;

    assert_eq!(
        client
            .execute(r#"DELETE FROM "Post" WHERE "published" = $1"#, &[&false])
            .await
            .unwrap(),
        20_000
    );
    count(&client, COUNT_ORPHANS, 0).await;
    assert_eq!(
        client.execute(r#"DELETE FROM "User""#, &[]).await.unwrap(),
        5_000
    );
    count(&client, COUNT_USERS, 0).await;
    count(&client, COUNT_POSTS, 0).await;
    count(&client, COUNT_ORPHANS, 0).await;

    client
        .batch_execute(
            "CREATE TABLE parent(id int PRIMARY KEY);
         CREATE TABLE child(id int PRIMARY KEY, pid int NOT NULL REFERENCES parent);
         CREATE INDEX child_pid ON child(pid);",
        )
        .await
        .unwrap();
    const ORPHANS: &str = "SELECT count(*) FROM child WHERE NOT EXISTS
        (SELECT 1 FROM parent WHERE parent.id = child.pid)";
    let migrated = client
        .query_one(
            "WITH inserted_parents AS (
            INSERT INTO parent SELECT g FROM generate_series(1,$1::int) g RETURNING id
         ), inserted_children AS (
            INSERT INTO child SELECT id,id FROM inserted_parents WHERE id <= $2::int RETURNING id
         )
         SELECT (SELECT count(*) FROM inserted_parents), count(*),
                (SELECT count(*) FROM parent), (SELECT count(*) FROM child)
         FROM inserted_children",
            &[&20_005i32, &10_000i32],
        )
        .await
        .unwrap();
    let counts: Vec<i64> = (0..4).map(|i| migrated.get(i)).collect();
    assert_eq!(counts, vec![20_005, 10_000, 0, 0]);
    count(&client, ORPHANS, 0).await;
    assert_eq!(
        client
            .execute(
                "MERGE INTO child USING (SELECT g FROM generate_series(1,$1::int) g) s ON child.id = s.g
         WHEN MATCHED THEN UPDATE SET pid = $1 + 1 - s.g
         WHEN NOT MATCHED THEN INSERT VALUES (s.g,s.g)",
                &[&20_005i32],
            )
            .await
            .unwrap(),
        20_005
    );
    count(&client, "SELECT count(*) FROM child", 20_005).await;
    count(&client, ORPHANS, 0).await;

    drop(client);
    task.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
