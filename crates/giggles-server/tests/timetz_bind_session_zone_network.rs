use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    pg: Running,
    path: PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "timetz-bind-session-zone-network-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed),
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
            connections: 16,
        };
        let listener = config.build().unwrap();
        let pg = serve_postgresql(config.listen, service, Arc::new(listener))
            .await
            .unwrap();
        Self { pg, path }
    }
    async fn connect(&self) -> TcpStream {
        let mut stream = TcpStream::connect(self.pg.address).await.unwrap();
        let mut startup = 196608u32.to_be_bytes().to_vec();
        startup.extend(b"user\0owner\0database\0postgres\0\0");
        stream.write_u32((startup.len() + 4) as u32).await.unwrap();
        stream.write_all(&startup).await.unwrap();
        loop {
            let frame = read(&mut stream).await;
            assert_ne!(frame.0, b'E', "startup failed: {:?}", frame.1);
            if frame.0 == b'Z' {
                return stream;
            }
        }
    }
    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

async fn send(stream: &mut TcpStream, tag: u8, body: &[u8]) {
    stream.write_u8(tag).await.unwrap();
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
    stream.flush().await.unwrap();
}
async fn read(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let tag = stream.read_u8().await.unwrap();
        let length = stream.read_u32().await.unwrap();
        assert!((4..=8 * 1024 * 1024).contains(&length));
        let mut body = vec![0; length as usize - 4];
        stream.read_exact(&mut body).await.unwrap();
        (tag, body)
    })
    .await
    .expect("wire response timeout")
}
fn cstr(value: &str, body: &mut Vec<u8>) {
    body.extend(value.as_bytes());
    body.push(0);
}

type Rows = Vec<Vec<Option<Vec<u8>>>>;

async fn cycle(stream: &mut TcpStream, context: &str) -> Rows {
    let mut rows = vec![];
    loop {
        let (tag, body) = read(stream).await;
        match tag {
            b'E' => panic!(
                "{context}: ErrorResponse {}",
                String::from_utf8_lossy(&body)
            ),
            b'D' => {
                let count = u16::from_be_bytes(body[..2].try_into().unwrap());
                let mut at = 2;
                let mut row = vec![];
                for _ in 0..count {
                    let length = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    at += 4;
                    if length == -1 {
                        row.push(None);
                    } else {
                        assert!(length >= 0, "{context}: invalid field length");
                        row.push(Some(body[at..at + length as usize].to_vec()));
                        at += length as usize;
                    }
                }
                assert_eq!(at, body.len(), "{context}: DataRow length");
                rows.push(row);
            }
            b'Z' => return rows,
            _ => {}
        }
    }
}

async fn simple(stream: &mut TcpStream, sql: &str) -> Rows {
    let mut body = vec![];
    cstr(sql, &mut body);
    send(stream, b'Q', &body).await;
    cycle(stream, sql).await
}

async fn prepare(stream: &mut TcpStream, name: &str, sql: &str, oids: &[u32]) {
    let mut body = vec![];
    cstr(name, &mut body);
    cstr(sql, &mut body);
    body.extend((oids.len() as u16).to_be_bytes());
    for oid in oids {
        body.extend(oid.to_be_bytes());
    }
    send(stream, b'P', &body).await;
    send(stream, b'S', &[]).await;
    assert!(
        cycle(stream, sql).await.is_empty(),
        "{sql}: Parse returned rows"
    );
}

async fn execute(stream: &mut TcpStream, name: &str, inputs: &[&str], binary: bool) -> Rows {
    let mut body = vec![0]; // unnamed portal
    cstr(name, &mut body);
    body.extend(0u16.to_be_bytes()); // text parameter inputs
    body.extend((inputs.len() as u16).to_be_bytes());
    for input in inputs {
        body.extend((input.len() as i32).to_be_bytes());
        body.extend(input.as_bytes());
    }
    body.extend(1u16.to_be_bytes());
    body.extend(u16::from(binary).to_be_bytes());
    send(stream, b'B', &body).await;
    send(stream, b'E', &[0, 0, 0, 0, 0]).await;
    send(stream, b'S', &[]).await;
    cycle(
        stream,
        &format!("execute {name}, binary={binary}, inputs={inputs:?}"),
    )
    .await
}

fn assert_text(rows: Rows, expected: &[&str], context: &str) {
    let expected: Rows = vec![
        expected
            .iter()
            .map(|value| Some(value.as_bytes().to_vec()))
            .collect(),
    ];
    assert_eq!(rows, expected, "{context}");
}

#[tokio::test]
async fn timetz_text_binds_follow_session_zone() {
    let fixture = Fixture::new().await;
    let mut stream = fixture.connect().await;
    simple(&mut stream, "CREATE TABLE timetz_binds (v timetz)").await;
    prepare(
        &mut stream,
        "insert_inferred",
        "INSERT INTO timetz_binds VALUES ($1)",
        &[0],
    )
    .await;
    prepare(
        &mut stream,
        "insert_declared",
        "INSERT INTO timetz_binds VALUES ($1)",
        &[1266],
    )
    .await;
    prepare(
        &mut stream,
        "update_inferred",
        "UPDATE timetz_binds SET v=$1",
        &[0],
    )
    .await;
    for zone in ["America/Chicago", "Asia/Kolkata", "UTC"] {
        simple(&mut stream, &format!("SET TIME ZONE '{zone}'")).await;
        for transaction in [false, true] {
            if transaction {
                simple(&mut stream, "BEGIN").await;
            }
            let expected = simple(&mut stream, "SELECT '01:02'::timetz::text").await;
            if zone == "Asia/Kolkata" {
                assert!(
                    std::str::from_utf8(expected[0][0].as_ref().unwrap())
                        .unwrap()
                        .ends_with("+05:30")
                );
            }
            for statement in ["insert_inferred", "insert_declared", "update_inferred"] {
                simple(&mut stream, "DELETE FROM timetz_binds").await;
                if statement == "update_inferred" {
                    simple(&mut stream, "INSERT INTO timetz_binds VALUES ('00:00+00')").await;
                }
                execute(&mut stream, statement, &["01:02"], false).await;
                assert_eq!(
                    simple(&mut stream, "SELECT v::text FROM timetz_binds").await,
                    expected,
                    "{zone}, transaction={transaction}, {statement}"
                );
            }
            simple(&mut stream, "DELETE FROM timetz_binds").await;
            execute(&mut stream, "insert_declared", &["12:00+05:30"], false).await;
            assert_text(
                simple(&mut stream, "SELECT v::text FROM timetz_binds").await,
                &["12:00:00+05:30"],
                "explicit offset",
            );
            if transaction {
                simple(&mut stream, "COMMIT").await;
            }
        }
    }
    drop(stream);
    fixture.stop().await;
}
