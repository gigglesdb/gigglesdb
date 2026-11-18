//! PostgreSQL 18.6 basis: named plans revalidate after another session commits DDL.
#![cfg(feature = "engine-rocksdb")]

use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::NoTls;

async fn case(label: &str, setup: &str, sql: &str, change: &str, expected: Option<&str>) {
    let path = std::env::temp_dir().join(format!("named-stale-{}-{label}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    for (name, superuser) in [("owner", true), ("r", false)] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                login: true,
                superuser,
                ..Default::default()
            })
            .unwrap();
    }
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
    let mut clients = Vec::new();
    let mut tasks = Vec::new();
    for role in ["owner", if label == "e" { "r" } else { "owner" }] {
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(pg.address.port())
            .user(role)
            .dbname("postgres")
            .connect(NoTls)
            .await
            .unwrap();
        clients.push(client);
        tasks.push(tokio::spawn(connection));
    }
    for sql in setup.split(';').filter(|sql| !sql.trim().is_empty()) {
        clients[0].batch_execute(sql).await.unwrap();
    }
    let statement = clients[1].prepare(sql).await.unwrap();
    clients[0].batch_execute(change).await.unwrap();
    let value = 7i32;
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
        if label == "f" { &[&value] } else { &[] };
    let result = clients[1].query(&statement, params).await;
    // Clean up before assertions, including when the unfixed server returns stale rows.
    drop(statement);
    drop(clients);
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    pg.shutdown().await.unwrap();
    drop(service);
    drop(admin);
    for attempt in 0..20 {
        match std::fs::remove_dir_all(&path) {
            Ok(()) => break,
            Err(error) if attempt == 19 => panic!("cleanup: {error}"),
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
        }
    }
    if let Some(code) = expected {
        let error = result.expect_err("stale named statement unexpectedly succeeded");
        let error = error.as_db_error().expect("database error");
        assert_eq!(error.code().code(), code, "{error}");
        if code == "0A000" {
            assert_eq!(error.message(), "cached plan must not change result type");
        }
    } else {
        let rows = result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
        assert_eq!(rows[0].columns()[0].name(), "a");
        assert_eq!(rows[0].get::<_, i32>(0), 7);
    }
}

#[tokio::test]
async fn star_add_column() {
    case(
        "a",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7)",
        "SELECT * FROM t",
        "ALTER TABLE t ADD COLUMN b int",
        Some("0A000"),
    )
    .await;
}
#[tokio::test]
async fn star_drop_column() {
    case(
        "b",
        "CREATE TABLE t(a int,b int); INSERT INTO t VALUES(7,8)",
        "SELECT * FROM t",
        "ALTER TABLE t DROP COLUMN b",
        Some("0A000"),
    )
    .await;
}
#[tokio::test]
async fn explicit_column_add_column() {
    case(
        "c",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7)",
        "SELECT a FROM t",
        "ALTER TABLE t ADD COLUMN b int",
        None,
    )
    .await;
}
#[tokio::test]
async fn drop_table() {
    case(
        "d",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7)",
        "SELECT a FROM t",
        "DROP TABLE t",
        Some("42P01"),
    )
    .await;
}
#[tokio::test]
async fn revoke_select() {
    case(
        "e",
        "CREATE TABLE t(a int); INSERT INTO t VALUES(7); GRANT SELECT ON t TO r",
        "SELECT a FROM t",
        "REVOKE SELECT ON t FROM r",
        Some("42501"),
    )
    .await;
}
#[tokio::test]
async fn insert_drop_column() {
    case(
        "f",
        "CREATE TABLE t(a int,b int)",
        "INSERT INTO t(a) VALUES($1)",
        "ALTER TABLE t DROP COLUMN a",
        Some("42703"),
    )
    .await;
}
