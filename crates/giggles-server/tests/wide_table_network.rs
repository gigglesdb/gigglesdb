use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::types::ToSql;

#[tokio::test]
async fn prepared_insert_and_read_of_three_hundred_columns() {
    const WIDTH: usize = 300;
    const ROWS: i32 = 1000;
    let path = std::env::temp_dir().join(format!("wide-table-network-{}", std::process::id()));
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
    let mut client_config = tokio_postgres::Config::new();
    client_config
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("postgres")
        .dbname("postgres");
    let (mut client, connection) = client_config.connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(connection);

    let columns = (1..=WIDTH)
        .map(|i| format!("c{i} int"))
        .collect::<Vec<_>>()
        .join(",");
    client
        .batch_execute(&format!("CREATE TABLE wide ({columns}, PRIMARY KEY (c1))"))
        .await
        .unwrap();
    let placeholders = (1..=WIDTH)
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(",");
    let insert = client
        .prepare(&format!("INSERT INTO wide VALUES ({placeholders})"))
        .await
        .unwrap();
    assert_eq!(insert.params().len(), WIDTH);

    let txn = client.transaction().await.unwrap();
    let tail: i32 = 42;
    let mut parameters: Vec<&(dyn ToSql + Sync)> = vec![&tail; WIDTH];
    let ids: Vec<_> = (1..=ROWS).collect();
    for id in &ids {
        parameters[0] = id;
        assert_eq!(txn.execute(&insert, &parameters).await.unwrap(), 1);
    }
    txn.commit().await.unwrap();

    // Read in key ranges so the same app stays within the legacy result budget.
    let select = client
        .prepare("SELECT * FROM wide WHERE c1 >= $1 AND c1 < $2 ORDER BY c1")
        .await
        .unwrap();
    assert_eq!(select.columns().len(), WIDTH);
    assert_eq!(select.columns()[WIDTH - 1].name(), "c300");
    let mut next = 1;
    for first in (1..=ROWS).step_by(100) {
        for row in client
            .query(&select, &[&first, &(first + 100)])
            .await
            .unwrap()
        {
            assert_eq!(row.len(), WIDTH);
            assert_eq!(row.get::<_, i32>(0), next);
            assert_eq!(row.get::<_, i32>(WIDTH - 1), tail);
            next += 1;
        }
    }
    assert_eq!(next - 1, ROWS);

    let limit = giggles_core::relational::MAX_TARGET_COLUMNS;
    let target_list = (0..limit)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let wide_select = format!("SELECT {target_list}");
    let statement = client.prepare(&wide_select).await.unwrap();
    assert_eq!(statement.columns().len(), limit);
    assert_eq!(
        client.query_one(&statement, &[]).await.unwrap().len(),
        limit
    );

    let error = client
        .batch_execute(&format!("{wide_select},0"))
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "54011");
    assert_eq!(
        error.message(),
        "target lists can have at most 1664 entries"
    );

    let columns = (1..=giggles_core::relational::MAX_TABLE_COLUMNS + 1)
        .map(|i| format!("c{i} int"))
        .collect::<Vec<_>>()
        .join(",");
    let error = client
        .batch_execute(&format!("CREATE TABLE too_wide ({columns})"))
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "54011");
    assert_eq!(error.message(), "tables can have at most 1600 columns");

    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
