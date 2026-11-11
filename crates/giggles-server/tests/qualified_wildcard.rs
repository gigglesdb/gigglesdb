//! DataGrip table editor statement over the PostgreSQL extended protocol.
#[path = "wire_support/mod.rs"]
#[allow(dead_code)]
mod wire_support;
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use wire_support::{bind, cstr, execute, parse, pg_read, pg_send, pg_sync};

// Exact bytes of captured query_id 8226a2fc35c71723ac677991297db65b2b11ae36386c0147570cfd2cf4ee33aa.
const SQL: &str = "SELECT t.*\nFROM public.table_name t\nLIMIT 501";

struct Fixture {
    pg: Running,
    path: PathBuf,
}

impl Fixture {
    async fn new(engine: Engine) -> Self {
        let path = std::env::temp_dir().join(format!(
            "qualified-wildcard-{engine:?}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&path).unwrap();
        let database = match engine {
            Engine::Legacy => path.join("db.redb"),
            Engine::V2 => path.join("db-v2"),
        };
        let admin = Admin::create_engine(database, Limits::default(), engine).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                superuser: true,
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        // Same loopback PostgreSQL listener as the existing authority/network tests.
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
        let pg = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
            .await
            .unwrap();
        Self { pg, path }
    }

    async fn connect(&self) -> TcpStream {
        let mut stream = TcpStream::connect(self.pg.address).await.unwrap();
        let mut startup = 196608u32.to_be_bytes().to_vec();
        for value in ["user", "owner", "database", "postgres", ""] {
            cstr(value, &mut startup);
        }
        stream.write_u32((startup.len() + 4) as u32).await.unwrap();
        stream.write_all(&startup).await.unwrap();
        loop {
            let (tag, body) = pg_read(&mut stream).await;
            assert_ne!(tag, b'E', "startup: {}", String::from_utf8_lossy(&body));
            if tag == b'Z' {
                return stream;
            }
        }
    }

    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

async fn exercise(engine: Engine) {
    assert_eq!(
        hex::encode(Sha256::digest(SQL.as_bytes())),
        "8226a2fc35c71723ac677991297db65b2b11ae36386c0147570cfd2cf4ee33aa"
    );
    let fixture = Fixture::new(engine).await;
    let mut stream = fixture.connect().await;
    for sql in [
        "CREATE TABLE table_name (test integer)",
        "INSERT INTO table_name VALUES (10),(20)",
    ] {
        let mut body = Vec::new();
        cstr(sql, &mut body);
        pg_send(&mut stream, b'Q', &body).await;
        loop {
            let (tag, body) = pg_read(&mut stream).await;
            assert_ne!(tag, b'E', "{sql}: {}", String::from_utf8_lossy(&body));
            if tag == b'Z' {
                break;
            }
        }
    }
    pg_send(&mut stream, b'P', &parse("datagrip", SQL)).await;
    pg_send(&mut stream, b'D', b"Sdatagrip\0").await;
    let described = pg_sync(&mut stream).await;
    assert_eq!(described.iter().map(|f| f.0).collect::<Vec<_>>(), b"1tTZ");
    let statement_description = described.iter().find(|f| f.0 == b'T').unwrap().1.clone();
    assert_description(&statement_description);
    pg_send(&mut stream, b'B', &bind("editor", "datagrip", &[], false)).await;
    pg_send(&mut stream, b'D', b"Peditor\0").await;
    pg_send(&mut stream, b'E', &execute("editor", 0)).await;
    let executed = pg_sync(&mut stream).await;
    assert_eq!(executed.iter().map(|f| f.0).collect::<Vec<_>>(), b"2TDDCZ");
    assert_eq!(executed[1].1, statement_description);
    let mut rows = executed
        .iter()
        .filter(|f| f.0 == b'D')
        .map(|(_, body)| {
            assert_eq!(u16::from_be_bytes(body[..2].try_into().unwrap()), 1);
            let length = i32::from_be_bytes(body[2..6].try_into().unwrap()) as usize;
            assert_eq!(body.len(), length + 6);
            String::from_utf8(body[6..].to_vec()).unwrap()
        })
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows, ["10", "20"]);
    // Unknown qualifiers use the same 42P01 mapping as missing FROM columns.
    pg_send(
        &mut stream,
        b'P',
        &parse("bad", "SELECT missing.* FROM table_name"),
    )
    .await;
    pg_send(&mut stream, b'D', b"Sbad\0").await;
    let failed = pg_sync(&mut stream).await;
    let error = &failed.iter().find(|f| f.0 == b'E').unwrap().1;
    assert!(error.windows(7).any(|field| field == b"C42P01\0"));
    drop(stream);
    fixture.stop().await;
}

fn assert_description(body: &[u8]) {
    assert_eq!(u16::from_be_bytes(body[..2].try_into().unwrap()), 1);
    assert_eq!(&body[2..7], b"test\0");
    assert_eq!(body.len(), 25);
    assert_eq!(u32::from_be_bytes(body[13..17].try_into().unwrap()), 23); // int4
    assert_eq!(i16::from_be_bytes(body[17..19].try_into().unwrap()), 4);
    assert_eq!(i32::from_be_bytes(body[19..23].try_into().unwrap()), -1);
    assert_eq!(i16::from_be_bytes(body[23..25].try_into().unwrap()), 0); // text
}
#[tokio::test]
async fn legacy_datagrip_qualified_wildcard() {
    exercise(Engine::Legacy).await;
}
#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn v2_datagrip_qualified_wildcard() {
    exercise(Engine::V2).await;
}
