use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{Service, auth::{PostgresqlConfig, TlsMode, serve_postgresql}, clock::Clock};
use std::{collections::BTreeMap, sync::Arc};
#[tokio::test]
async fn statement_count_and_sql_bytes_are_reclaimed() {
    let path = std::env::temp_dir().join(format!("cs2-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin.roles().create(NewRole { name: "owner".into(), login: true, superuser: true, ..Default::default() }).unwrap();
    let service = Service::new(admin, vec![], Clock::new(0,3600).unwrap()).unwrap();
    let config = PostgresqlConfig { listen: "127.0.0.1:0".parse().unwrap(), tls: TlsMode::Disabled,
        certificate: None, private_key: None, client_ca: None, certificates: BTreeMap::new(),
        databases: vec!["postgres".into()], loopback_trust: true, connections: 8 };
    let running = serve_postgresql(config.listen, service.clone(), Arc::new(config.build().unwrap())).await.unwrap();
    let (client, connection) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port())
        .user("owner").dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    // Matches the named prepared-statement cap in pgwire.rs.
    let mut statements = Vec::new();
    for _ in 0..1024 {
        statements.push(client.prepare("SELECT 1").await.unwrap());
    }
    assert_eq!(client.prepare("SELECT 1").await.unwrap_err().code().unwrap().code(), "54000");
    for statement in &statements {
        assert_eq!(client.query_one(statement, &[]).await.unwrap().get::<_, i32>(0), 1);
    }
    statements.pop();
    statements.push(client.prepare("SELECT 1").await.unwrap());
    statements.clear();
    client.simple_query("SELECT 1").await.unwrap();
    let sql = "SELECT 1";
    let fresh = client.prepare(sql).await.unwrap();
    client.simple_query("BEGIN").await.unwrap();
    for i in 0..1024 {
        client.simple_query(&format!("DECLARE c{i} CURSOR FOR SELECT 1")).await.unwrap();
    }
    assert_eq!(client.simple_query("DECLARE overflow CURSOR FOR SELECT 1").await.unwrap_err().code().unwrap().code(), "54000");
    client.simple_query("ROLLBACK").await.unwrap();
    client.simple_query("BEGIN; DECLARE fresh CURSOR FOR SELECT 1; CLOSE fresh; COMMIT").await.unwrap();
    drop(fresh);
    drop(statements);
    drop(client);
    connection.await.unwrap().unwrap();
    let (client, connection) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port())
        .user("owner").dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    let fresh = client.prepare(sql).await.unwrap();
    assert_eq!(client.query_one(&fresh, &[]).await.unwrap().get::<_, i32>(0), 1);
    drop(fresh);
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap(); drop(service);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn sql_byte_budget_is_reclaimed_on_close() {
    let path = std::env::temp_dir().join(format!("cs2-budget-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let admin = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
    admin.roles().create(NewRole { name: "owner".into(), login: true, superuser: true, ..Default::default() }).unwrap();
    let service = Service::new(admin, vec![], Clock::new(0,3600).unwrap()).unwrap();
    let config = PostgresqlConfig { listen: "127.0.0.1:0".parse().unwrap(), tls: TlsMode::Disabled,
        certificate: None, private_key: None, client_ca: None, certificates: BTreeMap::new(),
        databases: vec!["postgres".into()], loopback_trust: true, connections: 8 };
    let running = serve_postgresql(config.listen, service.clone(), Arc::new(config.build().unwrap())).await.unwrap();
    let (client, connection) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port())
        .user("owner").dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    client.batch_execute("CREATE TABLE bulk(id INTEGER PRIMARY KEY)").await.unwrap();
    let mut statements = Vec::new();
    let statement_bytes = giggles_core::relational::MAX_STATEMENT_SQL;
    let budget = (16 * 1024 * 1024).max(2 * statement_bytes);
    for _ in 0..budget / statement_bytes {
        let mut sql = "INSERT INTO bulk VALUES (1)".to_owned();
        sql.extend(std::iter::repeat_n(' ', statement_bytes - sql.len()));
        statements.push(client.prepare(&sql).await.unwrap());
    }
    let mut sql = "INSERT INTO bulk VALUES (1)".to_owned();
    sql.extend(std::iter::repeat_n(' ', statement_bytes - sql.len()));
    assert_eq!(client.prepare(&sql).await.unwrap_err().code().unwrap().code(), "54000");
    statements.pop();
    client.simple_query("SELECT 1").await.unwrap();
    statements.push(client.prepare(&sql).await.unwrap());
    drop(sql);
    drop(statements);
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
