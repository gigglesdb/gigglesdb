#![cfg(feature = "engine-rocksdb")]

use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::{NoTls, SimpleQueryMessage};

async fn bulk_case(rows: usize, boundaries: bool) {
    let path = std::env::temp_dir().join(format!("bulk-wire-{}-{rows}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
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
    let values = (0..rows)
        .map(|n| format!("({n})"))
        .collect::<Vec<_>>()
        .join(",");
    let insert = format!("INSERT INTO bulk VALUES {values}");
    // Exercise both protocol dispatches against the same statement shape.
    assert_eq!(client.execute(&insert, &[]).await.unwrap(), rows as u64);
    client.batch_execute("DELETE FROM bulk").await.unwrap();
    let result = client.simple_query(&insert).await.unwrap();
    assert!(
        result
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(n) if *n == rows as u64))
    );
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM bulk", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        rows as i64
    );
    if boundaries {
        assert_eq!(
            client
                .query_one(
                    "SELECT oid FROM pg_catalog.pg_type WHERE typname='int4'",
                    &[]
                )
                .await
                .unwrap()
                .get::<_, u32>(0),
            23
        );
        for sql in [
            "INSERT INTO bulk SELECT oid FROM pg_catalog.pg_type",
            "UPDATE bulk SET id=(SELECT oid FROM pg_catalog.pg_type WHERE typname='int4')",
            "DELETE FROM bulk WHERE id IN (SELECT oid FROM pg_catalog.pg_type)",
        ] {
            for extended in [false, true] {
                let error = if extended {
                    client.prepare(sql).await.unwrap_err()
                } else {
                    client.simple_query(sql).await.unwrap_err()
                };
                assert_eq!(error.as_db_error().unwrap().code().code(), "42501", "{sql}");
            }
        }
        // Admission now reaches core: conflict execution retains its 1,024-row
        // bound (separate from the paged append-only VALUES path).
        client
            .batch_execute("CREATE TABLE bulk_upsert(id INTEGER PRIMARY KEY, v INTEGER)")
            .await
            .unwrap();
        let pairs = (0..rows)
            .map(|n| format!("({n},0)"))
            .collect::<Vec<_>>()
            .join(",");
        let upsert = format!(
            "INSERT INTO bulk_upsert VALUES {pairs} ON CONFLICT(id) DO UPDATE SET v=excluded.v"
        );
        assert_eq!(
            client
                .execute(&upsert, &[])
                .await
                .unwrap_err()
                .as_db_error()
                .unwrap()
                .code()
                .code(),
            "54000"
        );
        let limit = giggles_core::relational::V2_MAX_SQL;
        let mut oversized = "INSERT INTO bulk VALUES (999999)".to_owned();
        oversized.extend(std::iter::repeat_n(' ', limit + 1 - oversized.len()));
        assert_eq!(oversized.len(), limit + 1);
        for extended in [false, true] {
            let error = if extended {
                client.prepare(&oversized).await.unwrap_err()
            } else {
                client.simple_query(&oversized).await.unwrap_err()
            };
            assert_eq!(error.as_db_error().unwrap().code().code(), "54000");
        }
        oversized.truncate(limit);
        assert_eq!(client.execute(&oversized, &[]).await.unwrap(), 1);
        client
            .execute("DELETE FROM bulk WHERE id=999999", &[])
            .await
            .unwrap();
        assert_eq!(
            client
                .query_one("SELECT count(*) FROM bulk", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            rows as i64
        );
    }
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
async fn two_thousand_rows_and_boundaries() {
    bulk_case(2_000, true).await;
}

#[tokio::test]
async fn twenty_thousand_rows() {
    bulk_case(20_000, false).await;
}
