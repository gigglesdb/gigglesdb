use giggles_core::{
    Admin, Engine, Limits,
    roles::{NewRole, SchemaPrivilege},
};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};

#[tokio::test]
async fn oracle_text_on_simple_and_extended_wire() {
    let path = std::env::temp_dir().join(format!(
        "explain-text-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
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
            ..Default::default()
        })
        .unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "other".into(),
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
    service
        .role_session("owner")
        .unwrap()
        .execute_sql(
            "CREATE TABLE ex(id int PRIMARY KEY, active boolean, v text)",
            &[],
        )
        .unwrap();
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
    for (sql, expected) in [
        ("SELECT 1", vec!["Result"]),
        (
            "SELECT id FROM ex WHERE active",
            vec!["Seq Scan on ex", "  Filter: active"],
        ),
        ("SELECT * FROM ex", vec!["Seq Scan on ex"]),
        (
            "SELECT * FROM ex LIMIT 5",
            vec!["Limit", "  ->  Seq Scan on ex"],
        ),
        (
            "SELECT * FROM ex WHERE id = 1",
            vec!["Index Scan using ex_pkey on ex", "  Index Cond: (id = 1)"],
        ),
        (
            "SELECT * FROM ex ORDER BY v",
            vec!["Sort", "  Sort Key: v", "  ->  Seq Scan on ex"],
        ),
        (
            "SELECT count(*) FROM ex",
            vec!["Aggregate", "  ->  Seq Scan on ex"],
        ),
        ("SELECT 1 + 1 AS two", vec!["Result"]),
        (
            "SELECT id FROM ex WHERE NOT active",
            vec!["Seq Scan on ex", "  Filter: (NOT active)"],
        ),
        (
            "INSERT INTO ex VALUES (1, true, 'a')",
            vec!["Insert on ex", "  ->  Result"],
        ),
        (
            "UPDATE ex SET v = 'b' WHERE id = 1",
            vec![
                "Update on ex",
                "  ->  Index Scan using ex_pkey on ex",
                "        Index Cond: (id = 1)",
            ],
        ),
        (
            "DELETE FROM ex WHERE active",
            vec![
                "Delete on ex",
                "  ->  Seq Scan on ex",
                "        Filter: active",
            ],
        ),
        (
            "SELECT * FROM ex WHERE v = 'a'",
            vec!["Seq Scan on ex", "  Filter: (v = 'a'::text)"],
        ),
    ] {
        let sql = format!("EXPLAIN (COSTS OFF) {sql};");
        let messages = client.simple_query(&sql).await.unwrap();
        let actual: Vec<_> = messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => {
                    assert_eq!(r.columns()[0].name(), "QUERY PLAN");
                    Some(r.get(0).unwrap())
                }
                _ => None,
            })
            .collect();
        assert_eq!(actual, expected, "simple {sql}");
        let statement = client.prepare(&sql).await.unwrap();
        assert_eq!(statement.columns()[0].name(), "QUERY PLAN");
        assert_eq!(
            *statement.columns()[0].type_(),
            tokio_postgres::types::Type::TEXT
        );
        let rows = client.query(&statement, &[]).await.unwrap();
        assert_eq!(
            rows.iter()
                .map(|r| r.get::<_, String>(0))
                .collect::<Vec<_>>(),
            expected,
            "extended {sql}"
        );
    }
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM ex", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0,
        "EXPLAIN must not execute its mutation"
    );
    for sql in [
        "SELECT * FROM ex WHERE v = 'a' AND id > 3",
        "SELECT * FROM ex ORDER BY id LIMIT 5",
        "SELECT v, count(*) FROM ex GROUP BY v",
        "SELECT * FROM ex WHERE id IN (1,2)",
    ] {
        let sql = format!("EXPLAIN (COSTS OFF) {sql}");
        for extended in [false, true] {
            let error = if extended {
                client.query(&sql, &[]).await.map(|_| ()).unwrap_err()
            } else {
                client.simple_query(&sql).await.map(|_| ()).unwrap_err()
            };
            assert_eq!(error.code().unwrap().code(), "0A000", "{sql}");
        }
    }
    for (sql, expected) in [
        ("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT 1", vec!["Result (actual rows=1.00 loops=1)"]),
        ("EXPLAIN (VERBOSE, COSTS OFF) SELECT 1 AS constant", vec!["Result", "  Output: 1"]),
        ("EXPLAIN (FORMAT JSON, COSTS OFF) SELECT 1", vec!["[\n  {\n    \"Plan\": {\n      \"Node Type\": \"Result\",\n      \"Parallel Aware\": false,\n      \"Async Capable\": false,\n      \"Disabled\": false\n    }\n  }\n]"]),
    ] {
        let messages = client.simple_query(sql).await.unwrap();
        let actual: Vec<_> = messages.iter().filter_map(|m| match m { tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0), _ => None }).collect();
        assert_eq!(actual, expected, "{sql}");
    }
    client.batch_execute("INSERT INTO ex VALUES (1,true,'old')").await.unwrap();
    client.batch_execute("BEGIN").await.unwrap();
    let rows = client.query("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) UPDATE ex SET v='new' WHERE id=1", &[]).await.unwrap();
    assert_eq!(rows.iter().map(|r| r.get::<_,String>(0)).collect::<Vec<_>>(), vec!["Update on ex (actual rows=0.00 loops=1)", "  ->  Index Scan using ex_pkey on ex (actual rows=1.00 loops=1)", "        Index Cond: (id = 1)", "        Index Searches: 1"]);
    assert_eq!(client.query_one("SELECT v FROM ex WHERE id=1", &[]).await.unwrap().get::<_,String>(0), "new");
    client.batch_execute("ROLLBACK").await.unwrap();
    client.batch_execute("GRANT UPDATE ON ex TO other").await.unwrap();
    assert_eq!(client.query_one("SELECT v FROM ex WHERE id=1", &[]).await.unwrap().get::<_,String>(0), "old");
    drop(client);
    connection.await.unwrap().unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("other")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    for sql in [
        "SELECT id FROM ex WHERE active",
        "SELECT * FROM ex WHERE v LIKE 'a'",
    ] {
        let sql = format!("EXPLAIN (COSTS OFF) {sql}");
        for extended in [false, true] {
            let error = if extended {
                client.query(&sql, &[]).await.map(|_| ()).unwrap_err()
            } else {
                client.simple_query(&sql).await.map(|_| ()).unwrap_err()
            };
            assert_eq!(error.code().unwrap().code(), "42501", "{sql}");
        }
    }
    client.simple_query("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) UPDATE ex SET v='constant'").await.unwrap();
    let error = client.simple_query("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) UPDATE ex SET v='constant' WHERE id=1").await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "42501");
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(service);
    for attempt in 0..20 {
        match std::fs::remove_dir_all(&path) {
            Ok(()) => break,
            Err(error) if attempt == 19 => panic!("cleanup: {error}"),
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
        }
    }
}
