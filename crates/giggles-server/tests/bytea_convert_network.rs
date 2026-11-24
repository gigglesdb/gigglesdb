use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc, time::UNIX_EPOCH};
use tokio_postgres::{NoTls, types::Type};

#[tokio::test]
async fn linear_outbox_backfill_over_pgwire() {
    let path = std::env::temp_dir().join(format!(
        "bytea-convert-network-{}-{}",
        std::process::id(),
        UNIX_EPOCH.elapsed().unwrap().as_nanos()
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..NewRole::default()
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
    let listener = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(listener.address.ip())
        .port(listener.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);

    client.batch_execute(r#"
        CREATE TABLE "LinearOutbox" (
            "id" integer PRIMARY KEY, "kind" text NOT NULL, "payload" jsonb NOT NULL, "dedupeKey" text
        );
        INSERT INTO "LinearOutbox" ("id", "kind", "payload") VALUES
            (1, 'LINK_ISSUE','{"issueRef":"ENG-1","source":"ui","target":{"kind":"doc","docId":"d1","elementId":"e1"}}'),
            (2, 'LINK_ISSUE','{"issueRef":"ENG-é \"q\"","source":null,"target":{"kind":"doc","docId":"d3","elementId":null}}'),
            (3, 'UNLINK','{"issueRef":"ENG-1"}');
    "#).await.unwrap();
    let updated = client.execute(r##"
        UPDATE "LinearOutbox"
        SET "dedupeKey" = encode(sha256(convert_to(jsonb_build_array(
          "payload"->>'issueRef', "payload"->>'source', "payload"#>>'{target,kind}',
          CASE WHEN "payload"#>>'{target,kind}' = 'doc' THEN "payload"#>>'{target,docId}' ELSE "payload"#>>'{target,objectId}' END,
          CASE WHEN "payload"#>>'{target,kind}' = 'doc' THEN COALESCE("payload"#>>'{target,elementId}', '') ELSE '' END
        )::text, 'UTF8')), 'hex')
        WHERE "kind" = 'LINK_ISSUE'
    "##, &[]).await.unwrap();
    assert_eq!(updated, 2);
    let rows = client
        .query(
            r#"SELECT "dedupeKey" FROM "LinearOutbox" ORDER BY "id""#,
            &[],
        )
        .await
        .unwrap();
    let keys = rows
        .iter()
        .map(|row| row.get::<_, Option<&str>>(0))
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        [
            Some("33c412376d7d9009a318578afd2fd032fe9af7c5a2254d9cba61ee1f29a4ecd7"),
            Some("657fc3b40985262aee68a715214f225e8f3e671fe280efd50fab0a8d462d5cbf"),
            None,
        ]
    );

    let builders = client
        .prepare("SELECT json_build_array(1), jsonb_build_array(1)")
        .await
        .unwrap();
    assert_eq!(*builders.columns()[0].type_(), Type::JSON);
    assert_eq!(*builders.columns()[1].type_(), Type::JSONB);

    for (sql, state, message) in [
        (
            "SELECT decode('0g', 'hex')",
            "22023",
            r#"invalid hexadecimal digit: "g""#,
        ),
        (
            r"SELECT convert_from('\xff', 'UTF8')",
            "22021",
            r#"invalid byte sequence for encoding "UTF8": 0xff"#,
        ),
        (
            r"SELECT convert_from('\x00', 'SQL_ASCII')",
            "22021",
            r#"invalid byte sequence for encoding "SQL_ASCII": 0x00"#,
        ),
    ] {
        for error in [
            client.simple_query(sql).await.unwrap_err(),
            client.query(sql, &[]).await.unwrap_err(),
        ] {
            let error = error.as_db_error().unwrap();
            assert_eq!(error.code().code(), state);
            assert_eq!(error.message(), message);
        }
    }

    drop(client);
    connection.await.unwrap().unwrap();
    listener.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
