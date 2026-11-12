#![cfg(feature = "engine-rocksdb")]

use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::NoTls;

async fn bulk_case(engine: Engine) {
    let path = std::env::temp_dir().join(format!(
        "prepared-rollback-{}-{engine:?}",
        std::process::id()
    ));
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..NewRole::default()
        })
        .unwrap();
    let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: BTreeMap::new(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 4,
    };
    let pg = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(pg.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    client
        .batch_execute("CREATE TABLE bulk (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    client.batch_execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES bulk(id), n INTEGER CHECK(n > 0)); INSERT INTO bulk VALUES(1)").await.unwrap();
    let begin = client.prepare("BEGIN").await.unwrap();
    let rollback = client.prepare("ROLLBACK").await.unwrap();
    let commit = client.prepare("COMMIT").await.unwrap();
    let save = client.prepare("SAVEPOINT expected").await.unwrap();
    let back = client.prepare("ROLLBACK TO expected").await.unwrap();
    let mut failures = Vec::new();
    let mut selected = None;
    let mut inserted = None;
    for round in 0..3 {
        client.execute(&begin, &[]).await.unwrap();
        if selected.is_none() {
            selected = Some(client.prepare("SELECT id FROM bulk").await.unwrap());
            inserted = Some(client.prepare("INSERT INTO bulk VALUES(2)").await.unwrap());
        }
        let statement = selected.as_ref().unwrap();
        client
            .execute(inserted.as_ref().unwrap(), &[])
            .await
            .unwrap();
        client.execute(&rollback, &[]).await.unwrap();
        client.execute(&begin, &[]).await.unwrap();
        client.query(statement, &[]).await.unwrap();
        client.execute(&save, &[]).await.unwrap();
        client.execute(&back, &[]).await.unwrap();
        client.execute(&commit, &[]).await.unwrap();
        for (sql, state) in [
            ("INSERT INTO bulk VALUES(1)", "23505"),
            ("INSERT INTO child VALUES(1,999,1)", "23503"),
            ("INSERT INTO child VALUES(1,1,0)", "23514"),
        ] {
            client.execute(&begin, &[]).await.unwrap();
            client.execute(&save, &[]).await.unwrap();
            if round == 0 {
                failures.push(client.prepare(sql).await.unwrap());
            }
            let index = match state {
                "23505" => 0,
                "23503" => 1,
                _ => 2,
            };
            let error = client.execute(&failures[index], &[]).await.unwrap_err();
            assert_eq!(
                error.as_db_error().unwrap().code().code(),
                state,
                "round {round}"
            );
            client.execute(&back, &[]).await.unwrap();
            client.query(statement, &[]).await.unwrap();
            client.execute(&commit, &[]).await.unwrap();
        }
    }
    client.execute(&begin, &[]).await.unwrap();
    client
        .batch_execute("CREATE TABLE transient (id INTEGER)")
        .await
        .unwrap();
    let transient = client.prepare("SELECT * FROM transient").await.unwrap();
    client.execute(&rollback, &[]).await.unwrap();
    assert_eq!(
        client
            .query(&transient, &[])
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .code()
            .code(),
        "42P01"
    );
    client.execute(&begin, &[]).await.unwrap();
    let changing = client.prepare("SELECT * FROM bulk").await.unwrap();
    client.execute(&commit, &[]).await.unwrap();
    client
        .batch_execute("ALTER TABLE bulk ADD COLUMN extra INTEGER")
        .await
        .unwrap();
    let error = client.query(&changing, &[]).await.unwrap_err();
    assert_eq!(error.as_db_error().unwrap().code().code(), "0A000");
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "cached plan must not change result type"
    );
    drop(client);
    connection.await.unwrap().unwrap();
    pg.shutdown().await.unwrap();
    drop(service);
    drop(admin);
    // The accepted connection task may finish dropping its RocksDB handle just
    // after listener shutdown; Windows still holds those files briefly.
    for attempt in 0..20 {
        match std::fs::remove_dir_all(&path) {
            Ok(()) => break,
            Err(error) if attempt == 19 => panic!("cleanup: {error}"),
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
        }
    }
}
#[tokio::test]
async fn stale_prepared_in_new_transaction() {
    bulk_case(Engine::V2).await;
    bulk_case(Engine::Legacy).await;
}
