use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::error::SqlState;

#[tokio::test]
async fn cross_type_update_enforces_no_action_and_cascade() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("fk-cross-type-{}-{nonce}", std::process::id()));
    let engine = if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    };
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
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
    let running = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);

    client
        .batch_execute(
            "CREATE TABLE p(k numeric(10,2) PRIMARY KEY);
         CREATE TABLE c(id int PRIMARY KEY, r int REFERENCES p);
         CREATE INDEX c_r ON c(r);
         INSERT INTO p VALUES (1);
         INSERT INTO c VALUES (1,1);",
        )
        .await
        .unwrap();
    let error = client
        .batch_execute("UPDATE p SET k=2 WHERE k=1")
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::FOREIGN_KEY_VIOLATION));
    let orphans: i64 = client
        .query_one(
            "SELECT count(*) FROM c WHERE r IS NOT NULL AND NOT EXISTS (SELECT 1 FROM p WHERE k=r)",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(orphans, 0);

    client
        .batch_execute(
            "ALTER TABLE c DROP CONSTRAINT c_r_fkey;
         ALTER TABLE c ADD FOREIGN KEY (r) REFERENCES p ON UPDATE CASCADE;
         UPDATE p SET k=2 WHERE k=1;",
        )
        .await
        .unwrap();
    let key: i32 = client
        .query_one("SELECT r FROM c", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(key, 2);

    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
