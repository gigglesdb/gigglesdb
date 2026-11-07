use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    service: Arc<Service>,
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sql-clock-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
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
        let listener = Arc::new(config.build().unwrap());
        let pg = serve_postgresql(config.listen, service.clone(), listener)
            .await
            .unwrap();
        Self { service, pg, path }
    }
    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        drop(self.service);
        // Listener shutdown waits for active connections to release the file.
        std::fs::remove_dir_all(self.path).unwrap();
    }
}
async fn wire_cycle(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut messages = Vec::new();
        loop {
            let tag = stream.read_u8().await.unwrap();
            let length = stream.read_u32().await.unwrap();
            assert!((4..65536).contains(&length));
            let mut body = vec![0; length as usize - 4];
            stream.read_exact(&mut body).await.unwrap();
            messages.push((tag, body));
            if tag == b'Z' {
                return messages;
            }
        }
    })
    .await
    .unwrap()
}
async fn wire_query(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut body = Vec::new();
    giggles_server::pgwire::z(sql, &mut body);
    let mut packet = Vec::new();
    giggles_server::pgwire::frame(b'Q', &body, &mut packet).unwrap();
    stream.write_all(&packet).await.unwrap();
    wire_cycle(stream).await
}

fn row(messages: &[(u8, Vec<u8>)]) -> Vec<String> {
    assert!(
        !messages.iter().any(|(tag, _)| *tag == b'E'),
        "{messages:?}"
    );
    let body = &messages.iter().find(|(tag, _)| *tag == b'D').unwrap().1;
    let count = u16::from_be_bytes(body[..2].try_into().unwrap());
    let mut at = 2;
    (0..count)
        .map(|_| {
            let len = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
            assert!(len >= 0);
            at += 4;
            let value = String::from_utf8(body[at..at + len as usize].to_vec()).unwrap();
            at += len as usize;
            value
        })
        .collect()
}
async fn execute_before_sync(stream: &mut TcpStream) -> Vec<String> {
    let mut parse = vec![0];
    giggles_server::pgwire::z("SELECT now(), statement_timestamp()", &mut parse);
    parse.extend_from_slice(&0u16.to_be_bytes());
    let mut packet = Vec::new();
    for (tag, body) in [
        (b'P', parse.as_slice()),
        (b'B', &b"\0\0\0\0\0\0\0\0"[..]),
        (b'E', &b"\0\0\0\0\0"[..]),
        (b'H', &b""[..]),
    ] {
        giggles_server::pgwire::frame(tag, body, &mut packet).unwrap();
    }
    stream.write_all(&packet).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut messages = Vec::new();
        loop {
            let tag = stream.read_u8().await.unwrap();
            let length = stream.read_u32().await.unwrap();
            assert!((4..65536).contains(&length));
            let mut body = vec![0; length as usize - 4];
            stream.read_exact(&mut body).await.unwrap();
            messages.push((tag, body));
            assert_ne!(tag, b'E', "{messages:?}");
            if tag == b'C' {
                return row(&messages);
            }
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn extended_cycle_pins_transaction_clock_and_sync_releases_it() {
    let f = Fixture::new().await;
    let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
    let body = b"\0\x03\0\0user\0owner\0database\0postgres\0\0";
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
    wire_cycle(&mut stream).await;
    let first = execute_before_sync(&mut stream).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let second = execute_before_sync(&mut stream).await;
    assert_eq!(first[0], second[0]);
    assert!(second[1] > first[1], "{first:?} {second:?}");
    stream.write_all(b"S\0\0\0\x04").await.unwrap();
    wire_cycle(&mut stream).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let third = execute_before_sync(&mut stream).await;
    assert!(third[0] > second[0], "{second:?} {third:?}");
    stream.write_all(b"S\0\0\0\x04").await.unwrap();
    wire_cycle(&mut stream).await;
    drop(stream);
    f.stop().await;
}
#[tokio::test]
async fn timezone_settings_drive_local_clocks_and_restore_after_transactions() {
    let f = Fixture::new().await;
    let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
    let body = b"\0\x03\0\0user\0owner\0database\0postgres\0TimeZone\0America/Chicago\0\0";
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
    wire_cycle(&mut stream).await;
    row(&wire_query(&mut stream, "SELECT CURRENT_DATE, LOCALTIMESTAMP").await);
    for set in ["SET TimeZone='America/Chicago'", "SET TimeZone='Not/AZone'"] {
        let result = wire_query(&mut stream, set).await;
        assert!(!result.iter().any(|(tag, _)| *tag == b'E'), "{result:?}");
        if set.contains("America/Chicago") {
            row(&wire_query(&mut stream, "SELECT CURRENT_DATE, LOCALTIMESTAMP").await);
        }
    }
    let failure = wire_query(&mut stream, "SELECT CURRENT_DATE").await;
    assert!(
        failure
            .iter()
            .any(|(tag, body)| *tag == b'E' && body.windows(7).any(|part| part == b"C0A000\0")),
        "{failure:?}"
    );
    row(&wire_query(&mut stream, "SELECT now()").await);
    for reset in ["RESET TimeZone", "RESET ALL"] {
        wire_query(&mut stream, reset).await;
        row(&wire_query(&mut stream, "SELECT CURRENT_DATE, LOCALTIMESTAMP").await);
        wire_query(&mut stream, "SET TimeZone='Not/AZone'").await;
    }
    wire_query(&mut stream, "SET TimeZone='America/Chicago'").await;
    for close in ["COMMIT", "ROLLBACK"] {
        wire_query(&mut stream, "BEGIN").await;
        wire_query(&mut stream, "SET LOCAL TimeZone='Not/AZone'").await;
        wire_query(&mut stream, close).await;
        row(&wire_query(&mut stream, "SELECT CURRENT_DATE, LOCALTIMESTAMP").await);
    }
    // An error in an implicit simple-query batch also restores the session zone.
    wire_query(
        &mut stream,
        "SET LOCAL TimeZone='Not/AZone'; SELECT CURRENT_DATE",
    )
    .await;
    row(&wire_query(&mut stream, "SELECT CURRENT_DATE, LOCALTIMESTAMP").await);
    // Check local wall values, rather than only observing that the calls succeed.
    wire_query(&mut stream, "BEGIN").await;
    wire_query(&mut stream, "SET LOCAL TimeZone='UTC'").await;
    let utc = row(&wire_query(&mut stream, "SELECT LOCALTIMESTAMP").await);
    wire_query(&mut stream, "SET LOCAL TimeZone='America/Chicago'").await;
    let chicago = row(&wire_query(&mut stream, "SELECT LOCALTIMESTAMP").await);
    assert_ne!(utc, chicago);
    wire_query(&mut stream, "ROLLBACK").await;
    drop(stream);
    f.stop().await;
}
