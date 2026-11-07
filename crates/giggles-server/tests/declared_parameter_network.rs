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
                for _ in 0..count {
                    at += body[at..].iter().position(|byte| *byte == 0).unwrap() + 1;
                    types.push(u32::from_be_bytes(
                        body[at + 6..at + 10].try_into().unwrap(),
                    ));
                    at += 18;
                }
                assert_eq!(at, body.len());
                response.columns.push(types);
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

fn assert_update(response: &Response, parameters: &[u32]) {
    assert!(
        response.errors.is_empty(),
        "{}: {response:?}",
        response.context
    );
    assert!(response.tags.contains(&b'1'), "{response:?}");
    assert!(response.tags.contains(&b'2'), "{response:?}");
    assert_eq!(response.parameters, parameters, "{response:?}");
    assert_eq!(response.commands, ["UPDATE 1"], "{response:?}");
    assert!(response.rows.is_empty(), "{response:?}");
}

#[tokio::test]
async fn update_declared_bytea_position_uses_bytes_for_text_and_binary_inputs() {
    let fixture = Fixture::new().await;
    seed_update_table(&fixture).await;
    for binary in [false, true] {
        let values: [Option<&[u8]>; 2] = if binary {
            [Some(&[0, 255]), Some(&[1, 0, 255, 2])]
        } else {
            [Some(br"\x00ff"), Some(br"\x0100ff02")]
        };
        let response = query(
            &fixture,
            "UPDATE t SET n=position($1 in $2) WHERE id=1",
            &[17, 17],
            binary,
            &values,
        )
        .await;
        assert_update(&response, &[17, 17]);
        query(&fixture, "SELECT n FROM t WHERE id=1", &[], false, &[])
            .await
            .success(&[], 23, Some(b"2"));
    }
    fixture.stop().await;
}

#[tokio::test]
async fn update_undeclared_position_keeps_text_semantics() {
    let fixture = Fixture::new().await;
    seed_update_table(&fixture).await;
    for (needle, haystack, expected) in [
        (&b"z"[..], "\u{e9}z".as_bytes(), &b"2"[..]),
        (&br"\x00ff"[..], &br"\x0100ff02"[..], &b"0"[..]),
    ] {
        let response = query(
            &fixture,
            "UPDATE t SET n=position($1 in $2) WHERE id=1",
            &[0, 0],
            false,
            &[Some(needle), Some(haystack)],
        )
        .await;
        assert_update(&response, &[25, 25]);
        query(&fixture, "SELECT n FROM t WHERE id=1", &[], false, &[])
            .await
            .success(&[], 23, Some(expected));
    }
    fixture.stop().await;
}

#[tokio::test]
async fn declared_bytea_position_uses_bytes_for_text_and_binary_inputs() {
    let fixture = Fixture::new().await;
    for binary in [false, true] {
        let values: [Option<&[u8]>; 2] = if binary {
            [Some(&[2]), Some(&[1, 2, 3])]
        } else {
            [Some(br"\x02"), Some(br"\x010203")]
        };
        let response = query(
            &fixture,
            "SELECT position($1 in $2)",
            &[17, 17],
            binary,
            &values,
        )
        .await;
        response.success(&[17, 17], 23, Some(b"2"));
    }
    let absent = query(
        &fixture,
        "SELECT position($1 in $2)",
        &[17, 17],
        false,
        &[Some(br"\x04"), Some(br"\x010203")],
    )
    .await;
    absent.success(&[17, 17], 23, Some(b"0"));
    let null = query(
        &fixture,
        "SELECT position($1 in $2)",
        &[17, 17],
        false,
        &[None, Some(br"\x010203")],
    )
    .await;
    null.success(&[17, 17], 23, None);
    fixture.stop().await;
}

#[tokio::test]
async fn undeclared_and_text_position_keep_text_semantics() {
    let fixture = Fixture::new().await;
    for oids in [&[][..], &[0, 0][..], &[25, 25][..]] {
        // These hex-looking strings do not contain one another as text.
        let response = query(
            &fixture,
            "SELECT position($1 in $2)",
            oids,
            false,
            &[Some(br"\x02"), Some(br"\x010203")],
        )
        .await;
        response.success(&[25, 25], 23, Some(b"0"));
        let response = query(
            &fixture,
            "SELECT position($1 in $2)",
            oids,
            false,
            &[Some(b"z"), Some("\u{e9}z".as_bytes())],
        )
        .await;
        response.success(&[25, 25], 23, Some(b"2"));
    }
    fixture.stop().await;
}

#[tokio::test]
async fn explicit_bytea_casts_and_bare_parameter_catalog_routing() {
    let fixture = Fixture::new().await;
    for oids in [&[][..], &[0, 0][..], &[17, 17][..]] {
        let response = query(
            &fixture,
            "SELECT position($1::bytea in $2::bytea)",
            oids,
            false,
            &[Some(br"\x02"), Some(br"\x010203")],
        )
        .await;
        response.success(&[17, 17], 23, Some(b"2"));
    }
    for binary in [false, true] {
        let value: &[u8] = if binary { &[1, 2, 3] } else { br"\x010203" };
        let response = query(&fixture, "SELECT $1", &[17], binary, &[Some(value)]).await;
        // The unchanged catalog keepalive route fixes this bare parameter as
        // text, so its existing wire declaration policy rejects OID 17.
        assert_eq!(response.errors, ["0A000"], "{response:?}");
        assert!(!response.tags.contains(&b'1'), "{response:?}");
        assert!(response.parameters.is_empty(), "{response:?}");
        assert!(response.columns.is_empty(), "{response:?}");
        assert!(response.rows.is_empty(), "{response:?}");
    }
    fixture.stop().await;
}

#[tokio::test]
async fn application_bytea_projection_keeps_wire_descriptions() {
    let fixture = Fixture::new().await;
    for binary in [false, true] {
        let value: &[u8] = if binary { &[1, 2, 3] } else { br"\x010203" };
        let response = query(&fixture, "SELECT $1::bytea", &[17], binary, &[Some(value)]).await;
        response.success(&[17], 17, Some(br"\x010203"));
    }
    fixture.stop().await;
}

#[tokio::test]
async fn declared_bytea_strpos_rejects_missing_overload() {
    let fixture = Fixture::new().await;
    for binary in [false, true] {
        let values: [Option<&[u8]>; 2] = if binary {
            [Some(&[1, 2, 3]), Some(&[2])]
        } else {
            [Some(br"\x010203"), Some(br"\x02")]
        };
        let response = query(&fixture, "SELECT strpos($1,$2)", &[17, 17], binary, &values).await;
        // Preserve the binder's existing text-argument type rejection.
        assert_eq!(response.errors, ["42804"], "{response:?}");
        assert!(!response.tags.contains(&b'1'), "{response:?}");
        assert!(response.rows.is_empty(), "{response:?}");
    }
    fixture.stop().await;
}

#[tokio::test]
async fn overlong_oid_declarations_keep_protocol_error() {
    let fixture = Fixture::new().await;
    for (sql, oids, values) in [
        (
            "SELECT position($1 in $2)",
            &[17, 17, 17][..],
            vec![Some(&b"a"[..]), Some(&b"abc"[..])],
        ),
        ("SELECT $1::text", &[17, 17][..], vec![Some(&b"a"[..])]),
    ] {
        let response = query(&fixture, sql, oids, false, &values).await;
        assert_eq!(response.errors, ["08P01"], "{response:?}");
        assert!(!response.tags.contains(&b'1'), "{response:?}");
        assert!(response.rows.is_empty(), "{response:?}");
    }
    fixture.stop().await;
}
