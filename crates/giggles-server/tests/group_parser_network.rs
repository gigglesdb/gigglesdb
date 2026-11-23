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
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{NoTls, SimpleQueryMessage};

#[tokio::test]
async fn truncated_group_clauses_leave_both_protocols_usable() {
    let path = std::env::temp_dir().join(format!(
        "group-parser-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            ..Default::default()
        })
        .unwrap();
    admin
        .roles()
        .grant_schema(
            "public",
            "owner",
            &[SchemaPrivilege::Create, SchemaPrivilege::Usage],
        )
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
        connections: 4,
    };
    let running = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let mut client_config = tokio_postgres::Config::new();
    client_config
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("owner")
        .dbname("postgres");

    let (client, connection) = client_config.connect(NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    client
        .batch_execute("CREATE TABLE t(a int, b int)")
        .await
        .unwrap();
    for (sql, message) in [
        // Known divergence: PostgreSQL echoes the spelling LIMIT.
        (
            "SELECT * FROM t group LIMIT 1",
            r#"syntax error at or near "limit""#,
        ),
        ("SELECT * FROM t group", "syntax error at end of input"),

        (
            "SELECT * FROM t group by having",
            r#"syntax error at or near "having""#,
        ),
        (
            "SELECT a FROM t group by rollup(",
            "syntax error at end of input",
        ),
    ] {
        let simple = client.simple_query(sql).await.unwrap_err();
        let extended = client.query(sql, &[]).await.unwrap_err();
        for error in [simple, extended] {
            let error = error
                .as_db_error()
                .expect("expected a PostgreSQL ErrorResponse");
            assert_eq!(error.code().code(), "42601", "{sql}");
            assert_eq!(error.message(), message, "{sql}");
        }
        let result = client.simple_query("SELECT 1;").await.unwrap();
        assert!(result.iter().any(
            |message| matches!(message, SimpleQueryMessage::Row(row) if row.get(0) == Some("1"))
        ));
        assert_eq!(client.query_one("SELECT 1", &[]).await.unwrap().len(), 1);
    }
    // PostgreSQL 18.6: `SELECT * FROM t group by;` -> 42601 syntax error at or near ";".
    // The simple-query splitter drops the terminator before core parsing, so the simple
    // protocol reports end of input (known message divergence); the SQLSTATE matches.
    let sql = "SELECT * FROM t group by;";
    let simple = client.simple_query(sql).await.unwrap_err();
    let simple = simple.as_db_error().expect("expected a PostgreSQL ErrorResponse");
    assert_eq!(simple.code().code(), "42601");
    assert!(
        matches!(simple.message(), "syntax error at end of input" | r#"syntax error at or near ";""#),
        "{}", simple.message()
    );
    let extended = client.query(sql, &[]).await.unwrap_err();
    let extended = extended.as_db_error().expect("expected a PostgreSQL ErrorResponse");
    assert_eq!(extended.code().code(), "42601");
    assert!(
        matches!(extended.message(), "syntax error at end of input" | r#"syntax error at or near ";""#),
        "{}", extended.message()
    );
    assert_eq!(client.query_one("SELECT 1", &[]).await.unwrap().len(), 1);
    let (other, other_connection) = client_config.connect(NoTls).await.unwrap();
    let other_connection = tokio::spawn(other_connection);
    let result = other.simple_query("SELECT 1").await.unwrap();
    assert!(
        result.iter().any(
            |message| matches!(message, SimpleQueryMessage::Row(row) if row.get(0) == Some("1"))
        )
    );

    drop(other);
    drop(client);
    other_connection.await.unwrap().unwrap();
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(service);
    for attempt in 0..20 {
        match std::fs::remove_dir_all(&path) {
            Ok(()) => break,
            Err(error) if attempt == 19 => panic!("cleanup: {error}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
}
