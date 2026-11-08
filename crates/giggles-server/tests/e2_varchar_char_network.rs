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
            "e2-varchar-char-network-{}-{}-{}",
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
#[derive(Default, Debug)]
struct Response {
    fields: Vec<(u32, i32)>,
    rows: Vec<Vec<Option<String>>>,
    error: Option<String>,
}
async fn query(stream: &mut TcpStream, sql: &str) -> Response {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    send(stream, b'Q', &body).await;
    let mut result = Response::default();
    loop {
        let (tag, body) = read(stream).await;
        match tag {
            b'T' => {
                let count = u16::from_be_bytes(body[..2].try_into().unwrap());
                let mut at = 2;
                for _ in 0..count {
                    at += body[at..].iter().position(|b| *b == 0).unwrap() + 1;
                    result.fields.push((
                        u32::from_be_bytes(body[at + 6..at + 10].try_into().unwrap()),
                        i32::from_be_bytes(body[at + 12..at + 16].try_into().unwrap()),
                    ));
                    at += 18;
                }
                assert_eq!(at, body.len());
            }
            b'D' => {
                let count = u16::from_be_bytes(body[..2].try_into().unwrap());
                let mut at = 2;
                let mut row = vec![];
                for _ in 0..count {
                    let len = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    at += 4;
                    if len == -1 {
                        row.push(None);
                    } else {
                        assert!(len >= 0);
                        row.push(Some(
                            String::from_utf8(body[at..at + len as usize].to_vec()).unwrap(),
                        ));
                        at += len as usize;
                    }
                }
                assert_eq!(at, body.len());
                result.rows.push(row);
            }
            b'E' => {
                let code = body
                    .split(|b| *b == 0)
                    .find(|field| field.first() == Some(&b'C'))
                    .unwrap();
                result.error = Some(String::from_utf8(code[1..].to_vec()).unwrap());
            }
            b'Z' => return result,
            _ => {}
        }
    }
}
fn text_rows(rows: &[&[&str]]) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|row| row.iter().map(|value| Some((*value).into())).collect())
        .collect()
}

#[test]
fn truncation_sqlstate_is_22001() {
    assert_eq!(
        giggles_server::sqlstate(&giggles_core::CoreError::Sql(
            giggles_core::relational::SqlError::StringDataRightTruncation,
        )),
        "22001"
    );
}

#[tokio::test]
async fn varchar_char_descriptions_catalog_and_truncation() {
    let fixture = Fixture::new().await;
    let mut stream = fixture.connect().await;
    for sql in [
        "CREATE TABLE t (v varchar(5), c char(3))",
        "INSERT INTO t VALUES ('hello', 'xy')",
    ] {
        assert_eq!(query(&mut stream, sql).await.error, None, "{sql}");
    }
    let result = query(&mut stream, "SELECT v,c FROM t").await;
    assert_eq!(result.error, None);
    assert_eq!(result.fields, vec![(1043, 9), (1042, 7)]);
    assert_eq!(result.rows, text_rows(&[&["hello", "xy "]]));
    let result = query(&mut stream, "SELECT a.attname,a.atttypid,a.atttypmod,format_type(a.atttypid,a.atttypmod) FROM pg_attribute a JOIN pg_class c ON a.attrelid=c.oid WHERE c.relname='t' AND a.attnum>0 AND a.attname IN ('v','c') ORDER BY a.attnum").await;
    assert_eq!(result.error, None);
    assert_eq!(
        result.rows,
        text_rows(&[
            &["v", "1043", "9", "character varying(5)"],
            &["c", "1042", "7", "character(3)"],
        ])
    );
    let result = query(&mut stream, "SELECT column_name,data_type,character_maximum_length FROM information_schema.columns WHERE table_name='t' AND column_name IN ('v','c') ORDER BY ordinal_position").await;
    assert_eq!(result.error, None);
    assert_eq!(
        result.rows,
        text_rows(&[&["v", "character varying", "5"], &["c", "character", "3"],])
    );
    let result = query(
        &mut stream,
        "SELECT format_type(1043,-1),format_type(1042,-1) FROM pg_database",
    )
    .await;
    assert_eq!(result.error, None);
    assert_eq!(result.rows, text_rows(&[&["character varying", "bpchar"]]));
    assert_eq!(
        query(&mut stream, "INSERT INTO t VALUES ('toolong', 'abc')")
            .await
            .error
            .as_deref(),
        Some("22001")
    );
    drop(stream);
    fixture.stop().await;
}
