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
            "upsert-expression-network-{}-{}-{}",
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
    send(&mut stream, b'E', &[0, 0, 0, 0, 0]).await;
    send(&mut stream, b'S', &[]).await;
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

async fn seed_update_table(fixture: &Fixture) {
    let mut stream = fixture.connect().await;
    for sql in [
        "CREATE TABLE t(id integer primary key, n integer)",
        "INSERT INTO t VALUES (1, 0)",
    ] {
        let mut body = vec![];
        cstr(sql, &mut body);
        send(&mut stream, b'Q', &body).await;
        loop {
            let (tag, body) = read(&mut stream).await;
            assert_ne!(tag, b'E', "{sql}: {body:?}");
            if tag == b'Z' {
                break;
            }
        }
    }
}

#[tokio::test]
async fn conflict_scalar_parameter_crosses_extended_protocol_bind() {
    let fixture = Fixture::new().await;
    seed_update_table(&fixture).await;
    for binary in [false, true] {
        let binary_value = 3i32.to_be_bytes();
        let parameter: &[u8] = if binary { &binary_value } else { b"3" };
        let response = query(
            &fixture,
            "INSERT INTO t VALUES (1, 2) ON CONFLICT(id) DO UPDATE SET n=t.n+excluded.n+$1 RETURNING n",
            &[23],
            binary,
            &[Some(parameter)],
        ).await;
        response.success(&[23], 23, Some(if binary { b"10" } else { b"5" }));
        assert_eq!(response.column_names, vec![vec!["n"], vec!["n"]]);
        assert_eq!(response.commands, ["INSERT 0 1"]);
    }
    let skipped = query(
        &fixture,
        "INSERT INTO t VALUES (1, 2) ON CONFLICT(id) DO UPDATE SET n=t.n+$1 WHERE FALSE RETURNING n",
        &[23], false, &[Some(b"9")],
    ).await;
    assert!(skipped.errors.is_empty(), "{skipped:?}");
    assert!(skipped.tags.contains(&b'2'));
    assert!(skipped.rows.is_empty());
    assert_eq!(skipped.commands, ["INSERT 0 0"]);
    fixture.stop().await;
}
