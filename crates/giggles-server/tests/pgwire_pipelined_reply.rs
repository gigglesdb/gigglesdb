//! GD-0082: replies to a pipelined extended-protocol batch leave the server in
//! one write, and a lone message still gets its reply without Sync.
//!
//! Message-by-message writes made each reply after the first wait for the
//! client's delayed ACK under Nagle (about 40 ms per query on Linux).
#[path = "wire_support/mod.rs"]
#[allow(dead_code)]
mod wire_support;
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use wire_support::{bind, cstr, execute, parse, pg_read};

struct Fixture {
    pg: Running,
    path: PathBuf,
}

impl Fixture {
    async fn new(test: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "pgwire-pipelined-{test}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin =
            Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::Legacy).unwrap();
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
        stream.set_nodelay(true).unwrap();
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

fn frame(tag: u8, body: &[u8], out: &mut Vec<u8>) {
    out.push(tag);
    out.extend(((body.len() + 4) as u32).to_be_bytes());
    out.extend(body);
}

/// Splits whole backend messages into their tags; panics on a partial one.
fn tags(bytes: &[u8]) -> Vec<u8> {
    let mut tags = vec![];
    let mut rest = bytes;
    while !rest.is_empty() {
        assert!(rest.len() >= 5, "partial message header: {rest:?}");
        let n = u32::from_be_bytes(rest[1..5].try_into().unwrap()) as usize;
        assert!(rest.len() > n, "partial message body: {rest:?}");
        tags.push(rest[0]);
        rest = &rest[1 + n..];
    }
    tags
}

#[tokio::test]
async fn pipelined_extended_batch_replies_in_one_write() {
    let fixture = Fixture::new("batch").await;
    let mut stream = fixture.connect().await;
    let mut batch = vec![];
    frame(b'P', &parse("", "SELECT 1"), &mut batch);
    frame(b'B', &bind("", "", &[], false), &mut batch);
    frame(b'D', b"P\0", &mut batch);
    frame(b'E', &execute("", 0), &mut batch);
    frame(b'S', &[], &mut batch);
    for _ in 0..5 {
        stream.write_all(&batch).await.unwrap();
        // One read returns whatever the first segment carried. Replies written
        // one message at a time arrive as ParseComplete alone.
        let mut first = vec![0; 64 * 1024];
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut first))
            .await
            .expect("reply timeout")
            .unwrap();
        assert_eq!(tags(&first[..n]), b"12TDCZ");
    }
    fixture.stop().await;
}

#[tokio::test]
async fn lone_messages_are_answered_without_sync() {
    let fixture = Fixture::new("lone").await;
    let mut stream = fixture.connect().await;
    // Parse with nothing behind it: the reply is not held for a later Sync.
    let mut one = vec![];
    frame(b'P', &parse("s1", "SELECT 1"), &mut one);
    stream.write_all(&one).await.unwrap();
    assert_eq!(pg_read(&mut stream).await.0, b'1');
    // Bind followed by Flush: both replies arrive before any Sync.
    let mut flush = vec![];
    frame(b'B', &bind("", "s1", &[], false), &mut flush);
    frame(b'H', &[], &mut flush);
    stream.write_all(&flush).await.unwrap();
    assert_eq!(pg_read(&mut stream).await.0, b'2');
    let mut sync = vec![];
    frame(b'S', &[], &mut sync);
    stream.write_all(&sync).await.unwrap();
    assert_eq!(pg_read(&mut stream).await.0, b'Z');
    fixture.stop().await;
}
