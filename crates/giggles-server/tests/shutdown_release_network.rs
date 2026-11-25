use giggles_core::{Admin, Engine, Limits, relational::Value, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
use tokio_postgres::{Client, NoTls};

fn create_database(engine: Engine, scenario: &str) -> (PathBuf, Admin) {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "shutdown-release-{scenario}-{engine:?}-{}-{nonce}",
        std::process::id()
    ));
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
    (path, admin)
}

fn service(admin: Admin) -> Arc<Service> {
    Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap()
}

async fn listen(service: Arc<Service>) -> Running {
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
    serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap()
}

async fn connect(
    running: &Running,
) -> (
    Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    (client, tokio::spawn(connection))
}

async fn reopen_after_writes(engine: Engine) {
    let (path, mut admin) = create_database(engine, "writes");
    for iteration in 0..20 {
        let service = service(admin);
        let running = listen(service.clone()).await;
        let (client, connection) = connect(&running).await;
        if iteration == 0 {
            client
                .batch_execute("CREATE TABLE reopen_rows(id int PRIMARY KEY)")
                .await
                .unwrap();
        }
        let first = iteration * 2;
        client
            .simple_query(&format!("INSERT INTO reopen_rows VALUES ({first})"))
            .await
            .unwrap();
        let second = first + 1;
        client.batch_execute("BEGIN").await.unwrap();
        let rows = client
            .query(
                "INSERT INTO reopen_rows VALUES ($1) RETURNING id",
                &[&second],
            )
            .await
            .unwrap();
        assert_eq!(rows[0].get::<_, i32>(0), second);
        client.batch_execute("COMMIT").await.unwrap();
        drop(client);
        connection.await.unwrap().unwrap();
        running.shutdown().await.unwrap();
        drop(service);

        admin = Admin::open(&path, Limits::default()).unwrap_or_else(|error| {
            panic!("reopen {engine:?} after iteration {iteration}: {error}")
        });
        let rows = admin
            .sql_session()
            .execute("SELECT id FROM reopen_rows ORDER BY id", &[], None)
            .unwrap()
            .rows;
        let ids: Vec<_> = rows.into_iter().map(|row| row.values).collect();
        let expected: Vec<_> = (0..=second).map(|id| vec![Value::Int(id.into())]).collect();
        assert_eq!(ids, expected);
    }
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

async fn reopen_while_statement_is_in_flight(engine: Engine) {
    let (path, admin) = create_database(engine, "in-flight");
    let service = service(admin);
    let entered = Arc::new(tokio::sync::Notify::new());
    let gate_entered = entered.clone();
    let (release, released) = oneshot::channel();
    let released = Mutex::new(Some(released));
    service
        .set_statement_gate(Arc::new(move || {
            gate_entered.notify_one();
            released
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .blocking_recv()
                .map_err(|error| error.to_string())
        }))
        .unwrap_or_else(|_| panic!("gate already installed"));

    let mut running = listen(service.clone()).await;
    let (client, connection) = connect(&running).await;
    let query = tokio::spawn(async move { client.query("SELECT 1", &[]).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert_eq!(service.pgwire_blocking_live(), 1);

    let idle = listen(service.clone()).await;
    tokio::time::timeout(Duration::from_secs(1), idle.shutdown())
        .await
        .unwrap()
        .unwrap();

    running.request_shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), connection)
        .await
        .unwrap()
        .unwrap();
    let mut shutdown = std::pin::pin!(running.shutdown());
    // Keep the worker parked if shutdown returns early, so the immediate open
    // below sees its lock. Release it only once shutdown has had time to wait.
    match tokio::time::timeout(Duration::from_secs(1), &mut shutdown).await {
        Ok(result) => result.unwrap(),
        Err(_) => {
            release.send(()).unwrap();
            shutdown.await.unwrap();
        }
    }
    drop(service);
    let reopened = Admin::open(&path, Limits::default()).unwrap_or_else(|error| {
        panic!("immediate {engine:?} reopen after in-flight shutdown: {error}")
    });
    assert!(query.await.unwrap().is_err());
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn legacy_reopens_after_writes() {
    reopen_after_writes(Engine::Legacy).await;
}

#[tokio::test]
async fn legacy_reopens_after_in_flight_shutdown() {
    reopen_while_statement_is_in_flight(Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn v2_reopens_after_writes() {
    reopen_after_writes(Engine::V2).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn v2_reopens_after_in_flight_shutdown() {
    reopen_while_statement_is_in_flight(Engine::V2).await;
}
