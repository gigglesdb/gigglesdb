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
async fn windows_see_all_grouping_sets_in_both_protocols() {
    let path = std::env::temp_dir().join(format!(
        "window-grouping-sets-{}-{}",
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
        .batch_execute(
            "CREATE TABLE t(a int, b text, v int); \
             INSERT INTO t VALUES (1,'x',10),(1,'y',20),(3,'x',30),(3,NULL,40),(NULL,'y',50);",
        )
        .await
        .unwrap();
    // PostgreSQL 18.6, with native NULL and integer output types on the binary path.
    let rollup = "SELECT a, grouping(a), count(*), count(*) OVER () FROM t \
                 GROUP BY ROLLUP(a) HAVING count(*) > $1 ORDER BY grouping(a), a";
    let cube = "SELECT a, b, grouping(a,b), count(*), count(*) OVER () FROM t \
               GROUP BY CUBE(a,b) HAVING count(*) > $1 ORDER BY grouping(a,b), a, b";
    assert_simple(
        &client,
        rollup,
        &[
            &[Some("1"), Some("0"), Some("2"), Some("3")],
            &[Some("3"), Some("0"), Some("2"), Some("3")],
            &[None, Some("1"), Some("5"), Some("3")],
        ],
    )
    .await;
    assert_simple(
        &client,
        cube,
        &[
            &[Some("1"), None, Some("1"), Some("2"), Some("5")],
            &[Some("3"), None, Some("1"), Some("2"), Some("5")],
            &[None, Some("x"), Some("2"), Some("2"), Some("5")],
            &[None, Some("y"), Some("2"), Some("2"), Some("5")],
            &[None, None, Some("3"), Some("5"), Some("5")],
        ],
    )
    .await;

    let rollup_rows: Vec<(Option<i32>, i32, i64, i64)> = client
        .query(rollup, &[&1_i64])
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect();
    assert_eq!(
        rollup_rows,
        [(Some(1), 0, 2, 3), (Some(3), 0, 2, 3), (None, 1, 5, 3)]
    );

    let cube_result = client.query(cube, &[&1_i64]).await.unwrap();
    let cube_rows: Vec<_> = cube_result
        .iter()
        .map(|row| {
            (
                row.get::<_, Option<i32>>(0),
                row.get::<_, Option<&str>>(1),
                row.get::<_, i32>(2),
                row.get::<_, i64>(3),
                row.get::<_, i64>(4),
            )
        })
        .collect();
    assert_eq!(
        cube_rows,
        [
            (Some(1), None, 1, 2, 5),
            (Some(3), None, 1, 2, 5),
            (None, Some("x"), 2, 2, 5),
            (None, Some("y"), 2, 2, 5),
            (None, None, 3, 5, 5),
        ]
    );

    drop(client);
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

async fn assert_simple(client: &tokio_postgres::Client, sql: &str, expected: &[&[Option<&str>]]) {
    let messages = client.simple_query(&sql.replace("$1", "1")).await.unwrap();
    let rows = messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => {
                Some((0..row.len()).map(|i| row.get(i)).collect::<Vec<_>>())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, expected, "{sql}");
}
