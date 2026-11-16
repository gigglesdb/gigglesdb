// v2-only: these tests need the RocksDB engine.
#![cfg(feature = "engine-rocksdb")]
//! R2-PGLIMITS-2: v2 DML RETURNING past 1 MiB streams every row over pgwire
//! (simple, extended and suspended portals) from the core spill file.
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc, time::SystemTime};
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

const ROWS: i32 = 50_000;

fn spill_files() -> usize {
    let prefix = format!("giggles-returning-{}-", std::process::id());
    std::fs::read_dir(std::env::temp_dir())
        .unwrap()
        .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with(&prefix))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v2_returning_past_one_mebibyte_streams_every_row() {
    let path = std::env::temp_dir().join(format!(
        "returning-spill-network-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create_engine(path.join("db-v2"), Limits::default(), Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole { name: "owner".into(), superuser: true, login: true, ..NewRole::default() })
        .unwrap();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let listen = PostgresqlConfig {
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
    let running = serve_postgresql(listen.listen, service, Arc::new(listen.build().unwrap())).await.unwrap();
    let mut config = Config::new();
    config.hostaddr(running.address.ip()).port(running.address.port()).user("owner").dbname("postgres");
    let (mut client, connection) = config.connect(NoTls).await.unwrap();
    let connection = tokio::spawn(connection);

    client.simple_query("CREATE TABLE t (id int PRIMARY KEY, v bigint, w text)").await.unwrap();
    client.simple_query("CREATE TABLE u (id int PRIMARY KEY, v bigint, w text)").await.unwrap();
    for start in (0..ROWS).step_by(10_000) {
        let values = (start..start + 10_000)
            .map(|i| format!("({i},{},'row-{i}')", i64::from(i) * 10))
            .collect::<Vec<_>>()
            .join(",");
        client.simple_query(&format!("INSERT INTO t VALUES {values}")).await.unwrap();
    }
    let expected = (0..ROWS).collect::<Vec<_>>();

    // Simple query protocol.
    let messages = client.simple_query("UPDATE t SET v = v + 1 RETURNING id, v, w").await.unwrap();
    let mut ids = Vec::new();
    for message in &messages {
        match message {
            SimpleQueryMessage::Row(row) => {
                let id: i32 = row.get(0).unwrap().parse().unwrap();
                assert_eq!(row.get(1).unwrap(), (i64::from(id) * 10 + 1).to_string());
                ids.push(id);
            }
            SimpleQueryMessage::CommandComplete(n) => assert_eq!(*n, ROWS as u64),
            _ => {}
        }
    }
    assert_eq!(ids, expected);

    // Extended protocol, unlimited Execute.
    let rows = client.query("UPDATE t SET v = v + 1 RETURNING id, w", &[]).await.unwrap();
    assert_eq!(rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), expected);
    assert_eq!(rows[42].get::<_, &str>(1), "row-42");

    let rows = client.query("INSERT INTO u SELECT id, v, w FROM t RETURNING id, v", &[]).await.unwrap();
    assert_eq!(rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), expected);
    assert_eq!(rows[9].get::<_, i64>(1), 92);

    // Extended protocol with Execute row limits: the portal suspends between batches.
    let tx = client.transaction().await.unwrap();
    let portal = tx.bind("DELETE FROM u RETURNING id", &[]).await.unwrap();
    let mut ids = Vec::new();
    let mut batches = 0;
    loop {
        let batch = tx.query_portal(&portal, 7_000).await.unwrap();
        if batch.is_empty() {
            break;
        }
        batches += 1;
        ids.extend(batch.iter().map(|r| r.get::<_, i32>(0)));
    }
    assert_eq!((ids, batches), (expected.clone(), 8));
    drop(portal);
    tx.commit().await.unwrap();
    let count = client.query_one("SELECT count(*) FROM u", &[]).await.unwrap();
    assert_eq!(count.get::<_, i64>(0), 0);

    // A spill held by an open portal is removed when the transaction rolls back.
    let tx = client.transaction().await.unwrap();
    let portal = tx.bind("UPDATE t SET v = -1 RETURNING id", &[]).await.unwrap();
    assert_eq!(tx.query_portal(&portal, 10).await.unwrap().len(), 10);
    assert_eq!(spill_files(), 1);
    tx.rollback().await.unwrap();
    let v = client.query_one("SELECT v FROM t WHERE id = 7", &[]).await.unwrap();
    assert_eq!(v.get::<_, i64>(0), 72);
    assert_eq!(spill_files(), 0);

    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(path);
}
