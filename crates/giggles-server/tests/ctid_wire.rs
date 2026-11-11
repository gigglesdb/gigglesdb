//! DataGrip ctid table editor statement over the PostgreSQL extended protocol.
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

// Exact bytes of the captured ctid SELECT.
const SQL: &str = "SELECT t.*, CTID\nFROM public.table_name t\nLIMIT 501";

struct Fixture {
    pg: Running,
    path: PathBuf,
}

impl Fixture {
    async fn new(engine: Engine) -> Self {
        let path = std::env::temp_dir().join(format!(
            "ctid-wire-{engine:?}-{}-{}",
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
        "4e65419fa8edc64246a9a4078a8d82cdb5f57a99af3e464d0cc67c4aea7431d9"
    );
    let fixture = Fixture::new(engine).await;
    let mut stream = fixture.connect().await;
    for sql in [
        "CREATE TABLE table_name (test integer)",
        "INSERT INTO table_name VALUES (10),(20),(30)",
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
    assert_eq!(executed.iter().map(|f| f.0).collect::<Vec<_>>(), b"2TDDDCZ");
    assert_eq!(executed[1].1, statement_description);
    let rows = executed
        .iter()
        .filter(|f| f.0 == b'D')
        .map(|(_, body)| {
            assert_eq!(u16::from_be_bytes(body[..2].try_into().unwrap()), 2);
            let mut offset = 2;
            let mut values = Vec::new();
            for _ in 0..2 {
                let length =
                    i32::from_be_bytes(body[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                values.push(
                    std::str::from_utf8(&body[offset..offset + length])
                        .unwrap()
                        .to_owned(),
                );
                offset += length;
            }
            assert_eq!(offset, body.len());
            values
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 3);
    let mut integers = rows.iter().map(|r| r[0].as_str()).collect::<Vec<_>>();
    integers.sort();
    assert_eq!(integers, ["10", "20", "30"]);
    let tids = rows
        .iter()
        .map(|r| r[1].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(tids.len(), 3);
    for tid in tids {
        let (block, offset) = tid
            .strip_prefix('(')
            .unwrap()
            .strip_suffix(')')
            .unwrap()
            .split_once(',')
            .unwrap();
        block.parse::<u32>().unwrap();
        assert_ne!(offset.parse::<u16>().unwrap(), 0);
    }
    drop(stream);
    fixture.stop().await;
}

fn assert_description(body: &[u8]) {
    assert_eq!(u16::from_be_bytes(body[..2].try_into().unwrap()), 2);
    let mut position = 2;
    let mut columns = Vec::new();
    for _ in 0..2 {
        let end = position + body[position..].iter().position(|b| *b == 0).unwrap();
        let name = std::str::from_utf8(&body[position..end]).unwrap();
        position = end + 1;
        let oid = u32::from_be_bytes(body[position + 6..position + 10].try_into().unwrap());
        assert_eq!(
            i32::from_be_bytes(body[position + 12..position + 16].try_into().unwrap()),
            -1
        );
        assert_eq!(
            i16::from_be_bytes(body[position + 16..position + 18].try_into().unwrap()),
            0
        );
        columns.push((name, oid));
        position += 18;
    }
    assert_eq!(columns, [("test", 23), ("ctid", 27)]);
    assert_eq!(position, body.len());
}
#[tokio::test]
async fn legacy_datagrip_ctid() {
    exercise(Engine::Legacy).await;
}
#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn v2_datagrip_ctid() {
    exercise(Engine::V2).await;
}
