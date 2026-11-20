#![cfg(feature = "engine-rocksdb")]
#![allow(dead_code)]
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
            "declared-parameter-network-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db"), Limits::default(), giggles_core::Engine::V2).unwrap();
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

#[derive(Debug)]
struct Response {
    context: String,
    tags: Vec<u8>,
    errors: Vec<String>,
    parameters: Vec<u32>,
    columns: Vec<Vec<u32>>,
    column_names: Vec<Vec<String>>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    commands: Vec<String>,
}
impl Response {
    fn success(&self, parameters: &[u32], column: u32, value: Option<&[u8]>) {
        assert!(self.errors.is_empty(), "{}: {self:?}", self.context);
        assert!(self.tags.contains(&b'1'), "{self:?}");
        assert!(self.tags.contains(&b'2'), "{self:?}");
        assert_eq!(self.parameters, parameters, "{self:?}");
        // Both statement and portal descriptions must agree with execution.
        assert_eq!(self.columns, vec![vec![column], vec![column]], "{self:?}");
        assert_eq!(self.rows, vec![vec![value.map(<[u8]>::to_vec)]], "{self:?}");
    }
}
async fn query(
    fixture: &Fixture,
    sql: &str,
    oids: &[u32],
    binary_parameters: bool,
    values: &[Option<&[u8]>],
) -> Response {
    let mut stream = fixture.connect().await;
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
    let chunked = oids.len() == 1000 && oids.iter().all(|oid| *oid == 0);
    send(&mut stream, b'E', &[0, 0, 0, 0, if chunked { 100 } else { 0 }]).await;
    send(&mut stream, if chunked { b'H' } else { b'S' }, &[]).await;
    let mut response = Response {
        context: format!("sql={sql:?}, oids={oids:?}, binary_parameters={binary_parameters}"),
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
            b'C' => {
                if chunked { send(&mut stream, b'S', &[]).await; }
                response.commands.push(
                std::str::from_utf8(body.strip_suffix(&[0]).expect("CommandComplete terminator"))
                    .unwrap()
                    .into(),
            ); },
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
            b's' if chunked => {
                send(&mut stream, b'E', &[0, 0, 0, 0, 100]).await;
                send(&mut stream, b'H', &[]).await;
            }
            b'Z' if chunked && response.tags.contains(&b's') && !response.tags.contains(&b'C') => {},
            b'Z' => return response,
            _ => {}
        }
    }
}

#[tokio::test]
async fn oid_zero_bigint_points() {
    let f = Fixture::new().await;
    for sql in ["CREATE TABLE t(id bigint PRIMARY KEY)".to_owned(), format!("INSERT INTO t VALUES {}", (1..=2000).map(|i|format!("({i})")).collect::<Vec<_>>().join(","))] {
        let r=query(&f,&sql,&[],false,&[]).await;
        assert!(r.errors.is_empty(), "{r:?}");
    }
    let sql=format!("SELECT id FROM t WHERE id IN ({})",(1..=1000).map(|i|format!("${i}")).collect::<Vec<_>>().join(","));
    let texts=(1..=1000).map(|i|i.to_string()).collect::<Vec<_>>();
    let values=texts.iter().map(|v|Some(v.as_bytes())).collect::<Vec<_>>();
    for oids in [vec![0;1000],vec![20;1000]] {
        let r=query(&f,&sql,&oids,false,&values).await;
        assert!(r.errors.is_empty(), "{r:?}");
        assert_eq!(r.parameters,vec![20;1000]);
        assert_eq!(r.rows.len(),1000);
        for (i,row) in r.rows.iter().enumerate() { assert_eq!(row[0].as_deref(),Some(texts[i].as_bytes())); }
        if oids[0]==0 { assert!(r.tags.contains(&b's')); }
    }
    let mut stream=f.connect().await;
    let mut simple=format!("SELECT id FROM t WHERE id IN ({})",texts.join(",")).into_bytes();
    simple.push(0);
    send(&mut stream,b'Q',&simple).await;
    let mut count=0;
    loop {
        let (tag,body)=read(&mut stream).await;
        assert_ne!(tag,b'E',"{body:?}");
        if tag==b'D' {count+=1;}
        if tag==b'Z' {break;}
    }
    assert_eq!(count,1000);
    drop(stream);
    f.stop().await;
}