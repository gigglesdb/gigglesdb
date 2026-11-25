use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn bulk_insert_tag_and_parameterized_union() {
    let path =
        std::env::temp_dir().join(format!("bulk-insert-select-network-{}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
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
    let mut socket = tokio::net::TcpStream::connect(running.address)
        .await
        .unwrap();
    let startup = b"\0\x03\0\0user\0postgres\0database\0postgres\0\0";
    socket.write_u32(startup.len() as u32 + 4).await.unwrap();
    socket.write_all(startup).await.unwrap();
    assert!(
        read_tags(&mut socket)
            .await
            .iter()
            .any(|(tag, _)| *tag == b'R')
    );
    for (sql, expected) in [
        (
            "CREATE TABLE t (id int PRIMARY KEY, n int)",
            b"CREATE TABLE\0".as_slice(),
        ),
        (
            "INSERT INTO t SELECT g, g FROM generate_series(1, 200000) g",
            b"INSERT 0 200000\0".as_slice(),
        ),
    ] {
        socket.write_u8(b'Q').await.unwrap();
        socket.write_u32(sql.len() as u32 + 5).await.unwrap();
        socket.write_all(sql.as_bytes()).await.unwrap();
        socket.write_u8(0).await.unwrap();
        assert_eq!(
            read_tags(&mut socket).await,
            vec![(b'C', expected.to_vec())]
        );
    }
    drop(socket);

    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("postgres")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let task = tokio::spawn(connection);
    let union = (1..=50)
        .map(|i| format!("SELECT id FROM t WHERE id = ${i}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let stmt = client
        .prepare(&format!("{union} ORDER BY id"))
        .await
        .unwrap();
    let values: Vec<i32> = (1..=50).rev().collect();
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = values
        .iter()
        .map(|n| n as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let ids = client
        .query(&stmt, &params)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get::<_, i32>(0))
        .collect::<Vec<_>>();
    assert_eq!(ids, (1..=50).collect::<Vec<_>>());
    let over_cap = std::iter::repeat_n("SELECT 1", 8193)
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let error = client.prepare(&over_cap).await.unwrap_err();
    let diagnostic = error.as_db_error().unwrap();
    assert_eq!(diagnostic.code().code(), "54001");
    assert_eq!(diagnostic.message(), "stack depth limit exceeded");
    assert!(diagnostic.hint().unwrap().contains("currently 2048kB"));
    drop(client);
    task.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

async fn read_tags(socket: &mut tokio::net::TcpStream) -> Vec<(u8, Vec<u8>)> {
    tokio::time::timeout(std::time::Duration::from_secs(90), async {
        let mut replies = vec![];
        loop {
            let tag = socket.read_u8().await.unwrap();
            let size = socket.read_u32().await.unwrap();
            let mut body = vec![0; (size - 4) as usize];
            socket.read_exact(&mut body).await.unwrap();
            if tag == b'Z' {
                return replies;
            }
            replies.push((tag, body));
        }
    })
    .await
    .expect("ReadyForQuery")
}
