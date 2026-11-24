use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

async fn command(socket: &mut TcpStream, sql: &str, expected: &[u8]) {
    socket.write_u8(b'Q').await.unwrap();
    socket.write_u32(sql.len() as u32 + 5).await.unwrap();
    socket.write_all(sql.as_bytes()).await.unwrap();
    socket.write_u8(0).await.unwrap();
    let result = replies(socket).await;
    assert_eq!(result, vec![(b'C', expected.to_vec())], "{sql}");
}

async fn replies(socket: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let mut replies = vec![];
        loop {
            let tag = socket.read_u8().await.unwrap();
            let size = socket.read_u32().await.unwrap();
            let mut body = vec![0; (size - 4) as usize];
            socket.read_exact(&mut body).await.unwrap();
            if tag == b'Z' {
                return replies;
            }
            replies.push((tag, body));
        }
    })
    .await
    .expect("ReadyForQuery")
}

#[tokio::test]
async fn migration_and_ctas_report_postgres_command_tags() {
    let path = std::env::temp_dir().join(format!(
        "insert-select-sources-network-{}",
        std::process::id()
    ));
    let engine = if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    };
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
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
    let address = running.address;
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(address.ip())
        .port(address.port())
        .user("postgres")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    client.batch_execute(r#"
        CREATE TABLE "Comment" (id uuid PRIMARY KEY, "commentThreadId" int,
            content text, "createdById" uuid, "deletedAt" bigint, "createdAt" bigint);
        CREATE TABLE "User" (id uuid PRIMARY KEY, "deletedAt" bigint);
        CREATE TABLE "CommentMention" ("commentId" uuid, "commentThreadId" int, "userId" uuid,
            "createdAt" bigint, PRIMARY KEY ("commentId", "userId"));
        INSERT INTO "User" VALUES
            ('b0000000-0000-0000-0000-000000000001',NULL),
            ('b0000000-0000-0000-0000-000000000002',NULL),
            ('b0000000-0000-0000-0000-000000000003',1);
        INSERT INTO "Comment" VALUES ('a0000000-0000-0000-0000-000000000001',7,
            '{b0000000-0000-0000-0000-000000000001,b0000000-0000-0000-0000-000000000001,b0000000-0000-0000-0000-000000000002,b0000000-0000-0000-0000-000000000003}',
            'b0000000-0000-0000-0000-000000000002',NULL,42);
        INSERT INTO "CommentMention" VALUES
            ('a0000000-0000-0000-0000-000000000001',7,'b0000000-0000-0000-0000-000000000004',42);
    "#).await.unwrap();

    let mut socket = TcpStream::connect(address).await.unwrap();
    let startup = b"\0\x03\0\0user\0postgres\0database\0postgres\0\0";
    socket.write_u32(startup.len() as u32 + 4).await.unwrap();
    socket.write_all(startup).await.unwrap();
    assert!(
        replies(&mut socket)
            .await
            .iter()
            .any(|(tag, _)| *tag == b'R')
    );
    let migration = r#"
        INSERT INTO "CommentMention" ("commentId","commentThreadId","userId","createdAt")
        SELECT DISTINCT c.id,c."commentThreadId",m.x::uuid,c."createdAt"
        FROM "Comment" c
        CROSS JOIN LATERAL unnest(c.content::text[]) AS m(x)
        JOIN "User" u ON u.id=m.x::uuid AND u."deletedAt" IS NULL
        WHERE c."deletedAt" IS NULL AND m.x::uuid<>c."createdById"
        ON CONFLICT ("commentId","userId") DO NOTHING
    "#;
    command(&mut socket, migration, b"INSERT 0 1\0").await;
    command(&mut socket, migration, b"INSERT 0 0\0").await;
    command(
        &mut socket,
        r#"CREATE TABLE recent AS SELECT "commentThreadId" AS thread
        FROM "CommentMention" WHERE "createdAt"=42 UNION ALL SELECT 8"#,
        b"SELECT 3\0",
    )
    .await;
    command(
        &mut socket,
        "CREATE TABLE empty AS SELECT * FROM recent WITH NO DATA",
        b"CREATE TABLE AS\0",
    )
    .await;
    command(
        &mut socket,
        "CREATE TABLE zero AS SELECT * FROM recent WHERE false",
        b"SELECT 0\0",
    )
    .await;
    assert_eq!(
        client
            .execute(
                "CREATE TABLE extended AS SELECT $1::int AS n UNION ALL SELECT 2",
                &[&1_i32]
            )
            .await
            .unwrap(),
        2
    );
    let recent: Vec<i32> = client
        .query("SELECT thread FROM recent ORDER BY thread", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(recent, [7, 7, 8]);
    let users: Vec<String> = client
        .query(
            r#"SELECT "userId"::text FROM "CommentMention" ORDER BY 1"#,
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        users,
        [
            "b0000000-0000-0000-0000-000000000001",
            "b0000000-0000-0000-0000-000000000004"
        ]
    );

    client
        .batch_execute("CREATE TABLE nn (id int, ids uuid[] NOT NULL)")
        .await
        .unwrap();
    let error = client.batch_execute("INSERT INTO nn SELECT 1,ARRAY['b0000000-0000-0000-0000-000000000001']::uuid[] UNION ALL SELECT 2,NULL").await.unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23502");
    assert_eq!(
        error.message(),
        "null value in column \"ids\" of relation \"nn\" violates not-null constraint"
    );
    assert!(
        client
            .query("SELECT * FROM nn", &[])
            .await
            .unwrap()
            .is_empty()
    );

    drop(socket);
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
