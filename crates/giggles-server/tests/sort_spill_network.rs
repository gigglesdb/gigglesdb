// v2-only: these tests need the RocksDB engine.
#![cfg(feature = "engine-rocksdb")]
//! R2-PGLIMITS-3: a v2 ORDER BY past work_mem spills sorted runs, and the
//! cursor streams the spilled result over pgwire; every row arrives in order.
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc, time::SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

const ROWS: i32 = 200_000;

fn spill_files() -> usize {
    let pid = std::process::id();
    std::fs::read_dir(std::env::temp_dir())
        .unwrap()
        .filter(|e| {
            let name = e.as_ref().unwrap().file_name().to_string_lossy().into_owned();
            name.starts_with(&format!("giggles-sort-{pid}-")) || name.starts_with(&format!("giggles-returning-{pid}-"))
        })
        .count()
}

async fn message(s: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let tag = s.read_u8().await.unwrap();
    let mut body = vec![0; s.read_u32().await.unwrap() as usize - 4];
    s.read_exact(&mut body).await.unwrap();
    (tag, body)
}

/// A permutation of 0..ROWS, NULL on every 1000th id.
fn key(id: i32) -> Option<i32> {
    (id % 1000 != 0).then(|| (i64::from(id) * 7919 % i64::from(ROWS)) as i32)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v2_order_by_past_work_mem_streams_every_row_in_order() {
    let path = std::env::temp_dir().join(format!(
        "sort-spill-network-{}-{}",
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

    client.simple_query("CREATE TABLE t (id int PRIMARY KEY, k int, w text)").await.unwrap();
    for start in (0..ROWS).step_by(10_000) {
        let values = (start..start + 10_000)
            .map(|i| format!("({i},{},'w-{:06}')", key(i).map_or("NULL".into(), |k| k.to_string()), i % 50_000))
            .collect::<Vec<_>>()
            .join(",");
        client.simple_query(&format!("INSERT INTO t VALUES {values}")).await.unwrap();
    }
    let mut desc_nulls_last = (0..ROWS).collect::<Vec<_>>();
    desc_nulls_last.sort_by_key(|id| (key(*id).is_none(), std::cmp::Reverse(key(*id))));

    // Extended protocol, int key DESC NULLS LAST.
    let rows = client.query("SELECT id, k FROM t ORDER BY k DESC NULLS LAST", &[]).await.unwrap();
    assert_eq!(rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), desc_nulls_last);
    assert!(rows.iter().all(|r| r.get::<_, Option<i32>>(1) == key(r.get(0))));
    assert_eq!(spill_files(), 0);

    // Simple protocol, text key with ties: stable in primary key order.
    let messages = client.simple_query("SELECT id, w FROM t ORDER BY w").await.unwrap();
    let mut ids = Vec::new();
    for message in &messages {
        match message {
            SimpleQueryMessage::Row(row) => ids.push(row.get(0).unwrap().parse::<i32>().unwrap()),
            SimpleQueryMessage::CommandComplete(n) => assert_eq!(*n, ROWS as u64),
            _ => {}
        }
    }
    let mut expected = (0..ROWS).collect::<Vec<_>>();
    expected.sort_by_key(|id| id % 50_000);
    assert_eq!(ids, expected);
    assert_eq!(spill_files(), 0);

    // COPY (SELECT ...) TO STDOUT streams the spilled result like PostgreSQL:
    // every row arrives in order (it used to be materialised, then capped).
    let mut s = tokio::net::TcpStream::connect(running.address).await.unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0owner\0database\0postgres\0\0");
    s.write_u32((startup.len() + 4) as u32).await.unwrap();
    s.write_all(&startup).await.unwrap();
    while message(&mut s).await.0 != b'Z' {}
    let sql = b"COPY (SELECT id, w FROM t ORDER BY w) TO STDOUT\0";
    s.write_u8(b'Q').await.unwrap();
    s.write_u32((sql.len() + 4) as u32).await.unwrap();
    s.write_all(sql).await.unwrap();
    let (mut copied, mut complete) = (Vec::new(), false);
    loop {
        match message(&mut s).await {
            (b'd', body) => copied.push(String::from_utf8(body).unwrap()),
            (b'C', body) => complete = body == format!("COPY {ROWS}\0").as_bytes(),
            (b'Z', _) => break,
            (b'H' | b'c', _) => {}
            (tag, body) => panic!("unexpected COPY message {tag}: {}", String::from_utf8_lossy(&body)),
        }
    }
    assert!(complete);
    let copied_ids = copied.iter().map(|line| line.split('\t').next().unwrap().parse::<i32>().unwrap()).collect::<Vec<_>>();
    assert_eq!(copied_ids, expected);
    drop(s);
    assert_eq!(spill_files(), 0);

    // Top-N is unchanged.
    let rows = client.query("SELECT id FROM t ORDER BY k DESC NULLS LAST LIMIT 10", &[]).await.unwrap();
    assert_eq!(rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), desc_nulls_last[..10]);

    // A portal read in batches, then closed early at transaction end, leaves no files.
    let tx = client.transaction().await.unwrap();
    let portal = tx.bind("SELECT id FROM t ORDER BY k DESC NULLS LAST", &[]).await.unwrap();
    let first = tx.query_portal(&portal, 50_000).await.unwrap();
    assert_eq!(first.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), desc_nulls_last[..50_000]);
    assert!(spill_files() > 0);
    drop(portal);
    tx.rollback().await.unwrap();
    assert_eq!(spill_files(), 0);

    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(path);
}
