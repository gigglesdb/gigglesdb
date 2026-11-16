#![cfg(feature = "engine-rocksdb")]

use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Config, NoTls, config::SslMode};

#[tokio::test]
async fn transaction_status_reaches_core_over_pgwire() {
    let path = std::env::temp_dir().join(format!(
        "xact-status-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create_engine(path.join("db"), Limits::default(), Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            superuser: true,
            login: true,
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
    let listener = Arc::new(config.build().unwrap());
    let pg = serve_postgresql(config.listen, service.clone(), listener)
        .await
        .unwrap();
    let (client, connection) = Config::new()
        .host("localhost")
        .hostaddr(pg.address.ip())
        .port(pg.address.port())
        .user("owner")
        .dbname("postgres")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("BEGIN; CREATE TABLE xs(id bigint PRIMARY KEY); INSERT INTO xs VALUES (1)")
        .await
        .unwrap();
    let committed: i64 = client
        .query_one("SELECT pg_current_xact_id()", &[])
        .await
        .unwrap()
        .get(0);
    assert!(committed > 0);
    let assigned: i64 = client
        .query_one("SELECT pg_current_xact_id_if_assigned()", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(assigned, committed);
    client.batch_execute("COMMIT").await.unwrap();
    let status: String = client
        .query_one("SELECT pg_xact_status($1)", &[&committed])
        .await
        .unwrap()
        .get(0);
    assert_eq!(status, "committed");
    client
        .batch_execute("BEGIN; INSERT INTO xs VALUES (2)")
        .await
        .unwrap();
    let aborted: i64 = client
        .query_one("SELECT pg_current_xact_id()", &[])
        .await
        .unwrap()
        .get(0);
    assert!(aborted > committed);
    client.batch_execute("ROLLBACK").await.unwrap();
    let status: String = client
        .query_one("SELECT pg_xact_status($1)", &[&aborted])
        .await
        .unwrap()
        .get(0);
    assert_eq!(status, "aborted");
    drop(client);
    connection.await.unwrap();
    pg.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
