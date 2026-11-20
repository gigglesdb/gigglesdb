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
    async fn new(engine: giggles_core::Engine) -> Self {
        let path = std::env::temp_dir().join(format!(
            "declared-parameter-network-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap();
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
    tokio::time::timeout(Duration::from_secs(120), async {
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

#[derive(Debug)]
struct Response {
    tags: Vec<u8>,
    errors: Vec<String>,
    parameters: Vec<u32>,
    columns: Vec<Vec<u32>>,
    column_names: Vec<Vec<String>>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    commands: Vec<String>,
}
async fn query(
    fixture: &Fixture,
    sql: &str,
    oids: &[u32],
    binary_parameters: bool,
    values: &[Option<&[u8]>],
) -> Response {
    query_with_setting(fixture, sql, oids, binary_parameters, values, None).await
}

async fn query_with_setting(
    fixture: &Fixture,
    sql: &str,
    oids: &[u32],
    binary_parameters: bool,
    values: &[Option<&[u8]>],
    setting: Option<&str>,
) -> Response {
    let mut stream = fixture.connect().await;
    if let Some(setting) = setting {
        let mut body = vec![];
        cstr(setting, &mut body);
        send(&mut stream, b'Q', &body).await;
        loop {
            let (tag, body) = read(&mut stream).await;
            assert_ne!(tag, b'E', "SET failed: {body:?}");
            if tag == b'Z' { break; }
        }
    }
    let mut parse = vec![0];
    cstr(sql, &mut parse);
    parse.extend((oids.len() as u16).to_be_bytes());
    for oid in oids {
        parse.extend(oid.to_be_bytes());
    }
    send(&mut stream, b'P', &parse).await;
    send(&mut stream, b'D', &[b'S', 0]).await;
    let mut bind = vec![0, 0];
    bind.extend(1u16.to_be_bytes());
    bind.extend(u16::from(binary_parameters).to_be_bytes());
    bind.extend((values.len() as u16).to_be_bytes());
    for value in values {
        if let Some(value) = value {
            bind.extend((value.len() as i32).to_be_bytes());
            bind.extend(*value);
        } else {
            bind.extend((-1i32).to_be_bytes());
        }
    }
    // Request text results regardless of the parameter format.
    bind.extend(0u16.to_be_bytes());
    send(&mut stream, b'B', &bind).await;
    send(&mut stream, b'D', &[b'P', 0]).await;
    send(&mut stream, b'E', &[0, 0, 0, 0, 0]).await;
    send(&mut stream, b'S', &[]).await;
    let mut response = Response {
        tags: vec![],
        errors: vec![],
        parameters: vec![],
        columns: vec![],
        column_names: vec![],
        rows: vec![],
        commands: vec![],
    };
    loop {
        let (tag, body) = read(&mut stream).await;
        response.tags.push(tag);
        match tag {
            b'C' => response.commands.push(
                std::str::from_utf8(body.strip_suffix(&[0]).expect("CommandComplete terminator"))
                    .unwrap()
                    .into(),
            ),
            b'E' => {
                let code = body
                    .split(|byte| *byte == 0)
                    .find(|field| field.first() == Some(&b'C'))
                    .expect("ErrorResponse SQLSTATE");
                response
                    .errors
                    .push(std::str::from_utf8(&code[1..]).unwrap().into());
            }
            b't' => {
                let count = u16::from_be_bytes(body[..2].try_into().unwrap()) as usize;
                assert_eq!(body.len(), 2 + 4 * count);
                response.parameters = body[2..]
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|bytes| u32::from_be_bytes(*bytes))
                    .collect();
            }
            b'T' => {
                let count = u16::from_be_bytes(body[..2].try_into().unwrap());
                let mut at = 2;
                let mut types = vec![];
                let mut names = vec![];
                for _ in 0..count {
                    let end = at + body[at..].iter().position(|byte| *byte == 0).unwrap();
                    names.push(std::str::from_utf8(&body[at..end]).unwrap().to_owned());
                    at = end + 1;
                    types.push(u32::from_be_bytes(
                        body[at + 6..at + 10].try_into().unwrap(),
                    ));
                    at += 18;
                }
                assert_eq!(at, body.len());
                response.columns.push(types);
                response.column_names.push(names);
            }
            b'D' => {
                let count = u16::from_be_bytes(body[..2].try_into().unwrap());
                let mut at = 2;
                let mut row = vec![];
                for _ in 0..count {
                    let length = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    at += 4;
                    if length < 0 {
                        row.push(None);
                    } else {
                        row.push(Some(body[at..at + length as usize].to_vec()));
                        at += length as usize;
                    }
                }
                assert_eq!(at, body.len());
                response.rows.push(row);
            }
            b'Z' => return response,
            _ => {}
        }
    }
}

#[tokio::test]
async fn many_parameters() {
 for engine in [giggles_core::Engine::Legacy, #[cfg(feature="engine-rocksdb")] giggles_core::Engine::V2] {
 let fixture=Fixture::new(engine).await;
 let response=query(&fixture,"CREATE TABLE t(id int PRIMARY KEY, n int)",&[],false,&[]).await;
 assert!(response.errors.is_empty(),"{response:?}");
 let response=query(&fixture,"INSERT INTO t VALUES(1,1),(2,2)",&[],false,&[]).await;
 assert!(response.errors.is_empty(),"{response:?}");
 for n in [257,1000,2044,2045] {
 if engine==giggles_core::Engine::Legacy && n>7000 {continue;}
 let sql=format!("SELECT id FROM t WHERE id IN ({})",(1..=n).map(|i|format!("${i}")).collect::<Vec<_>>().join(","));
 let values=vec![Some(b"1".as_slice());n];
 let response=query(&fixture,&sql,&vec![23;n],false,&values).await;
 // Legacy retains the fixed cap; v2 uses the session statement budget.
 if engine==giggles_core::Engine::Legacy && n>=2045 {assert_eq!(response.errors,vec!["54000"]);} else {assert!(response.errors.is_empty(),"{response:?}");assert_eq!(response.rows.len(),1);}
 if engine!=giggles_core::Engine::Legacy && n==2045 {
 let response=query_with_setting(&fixture,&sql,&vec![23;n],false,&values,Some("SET gigglesdb.max_statement_tokens = 4096")).await;
 assert_eq!(response.errors,vec!["54000"]);
 }
 }
 let response=query(&fixture,"CREATE TABLE batch(a int, b int)",&[],false,&[]).await;
 assert!(response.errors.is_empty());
 let (mut lo,mut hi)=(0,1101);
 while lo+1<hi {
 let n=(lo+hi)/2;
 let sql=format!("INSERT INTO batch VALUES {}",(0..n).map(|i|format!("(${},${})",2*i+1,2*i+2)).collect::<Vec<_>>().join(","));
 let response=query(&fixture,&sql,&vec![23;2*n],false,&vec![Some(b"1".as_slice());2*n]).await;
 if response.errors.is_empty() {lo=n;} else {assert_eq!(response.errors,vec!["54000"]);hi=n;}
 }
 eprintln!("F13 INSERT {engine:?}: largest two-column rows={lo}, params={}",lo*2);
 assert!(lo>=500);
 let sql=format!("DELETE FROM t WHERE id IN ({})",(1..=1000).map(|i|format!("${i}")).collect::<Vec<_>>().join(","));
 let response=query(&fixture,&sql,&vec![23;1000],false,&vec![Some(b"1".as_slice());1000]).await;assert!(response.errors.is_empty(),"{response:?}");
 fixture.stop().await;
 }
}
