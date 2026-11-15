#![cfg(feature = "engine-rocksdb")]
use giggles_core::{
    Admin, Engine, Limits,
    roles::{NewRole, SchemaPrivilege},
};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::Duration,
};
use tokio_postgres::NoTls;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadlock_returns_40p01_and_survivor_commits() {
    let path = std::env::temp_dir().join(format!("f19-{}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    admin.roles().create(NewRole { name: "owner".into(), login: true, ..NewRole::default() }).unwrap();
    admin.roles().grant_schema("public", "owner", &[SchemaPrivilege::Create, SchemaPrivilege::Usage]).unwrap();
    let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(), tls: TlsMode::Disabled,
        certificate: None, private_key: None, client_ca: None, certificates: BTreeMap::new(),
        databases: vec!["postgres".into()], loopback_trust: true, connections: 4,
    };
    let pg = serve_postgresql(config.listen, service.clone(), Arc::new(config.build().unwrap())).await.unwrap();
    let mut clients = Vec::new();
    for _ in 0..2 {
        let (client, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(pg.address.port()).user("owner").dbname("postgres").connect(NoTls).await.unwrap();
        tokio::spawn(async move { let _ = connection.await; });
        clients.push(client);
    }
    let a = &clients[0]; let b = &clients[1];
    a.batch_execute("CREATE TABLE deadlock_rows (id BIGINT PRIMARY KEY, v BIGINT); INSERT INTO deadlock_rows VALUES (1,0),(2,0)").await.unwrap();
    a.batch_execute("BEGIN; UPDATE deadlock_rows SET v=1 WHERE id=1").await.unwrap();
    b.batch_execute("BEGIN; UPDATE deadlock_rows SET v=2 WHERE id=2").await.unwrap();
    let outcomes = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(async {
            let result = a.execute("UPDATE deadlock_rows SET v=1 WHERE id=2", &[]).await;
            if result.is_ok() { a.batch_execute("COMMIT").await.unwrap(); } else {
                assert_eq!(a.simple_query("SELECT 1").await.unwrap_err().code().unwrap().code(), "25P02");
                a.batch_execute("ROLLBACK").await.unwrap();
            }
            result
        }, async {
            let result = b.execute("UPDATE deadlock_rows SET v=2 WHERE id=1", &[]).await;
            if result.is_ok() { b.batch_execute("COMMIT").await.unwrap(); } else {
                assert_eq!(b.simple_query("SELECT 1").await.unwrap_err().code().unwrap().code(), "25P02");
                b.batch_execute("ROLLBACK").await.unwrap();
            }
            result
        })
    }).await.expect("deadlock must resolve");
    let results = [outcomes.0, outcomes.1];
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    let victim = results.iter().position(|r| r.is_err()).unwrap();
    assert_eq!(results[victim].as_ref().unwrap_err().code().unwrap().code(), "40P01");
    drop(clients);
    pg.shutdown().await.unwrap();
    drop(service); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
