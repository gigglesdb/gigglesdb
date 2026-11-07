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
    let response = query(&fixture, "SELECT $1", &[], false, &[Some(br"\x010203")]).await;
    response.success(&[25], 25, Some(br"\x010203"));
    assert_eq!(
        response.column_names,
        vec![vec!["?column?"], vec!["?column?"]]
    );
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
        response.success(&[17], 17, Some(br"\x010203"));
        assert_eq!(
            response.column_names,
            vec![vec!["?column?"], vec!["?column?"]]
        );
    }
    fixture.stop().await;
}

#[tokio::test]
async fn mixed_declared_bytea_and_unknown_bare_parameters() {
    let fixture = Fixture::new().await;
    let response = query(
        &fixture,
        "SELECT $1, $2",
        &[17, 0],
        false,
        &[Some(br"\x010203"), Some(b"hello")],
    )
    .await;
    assert!(response.errors.is_empty(), "{response:?}");
    assert_eq!(response.parameters, [17, 25], "{response:?}");
    assert_eq!(
        response.columns,
        vec![vec![17, 25], vec![17, 25]],
        "{response:?}"
    );
    assert_eq!(
        response.column_names,
        vec![vec!["?column?", "?column?"], vec!["?column?", "?column?"]]
    );
    assert_eq!(
        response.rows,
        vec![vec![Some(br"\x010203".to_vec()), Some(b"hello".to_vec())]],
        "{response:?}"
    );
    fixture.stop().await;
}

#[tokio::test]
async fn text_declared_parameter_cast_to_bytea() {
    let fixture = Fixture::new().await;
    for (input, output) in [
        (&b"abc"[..], &br"\x616263"[..]),
        (b"A", br"\x41"),
        (br"\x41", br"\x41"),
    ] {
        let response = query(&fixture, "SELECT $1::bytea", &[25], false, &[Some(input)]).await;
        response.success(&[25], 17, Some(output));
    }
    for (input, error) in [(&br"\x4"[..], "22023"), (&br"\zz"[..], "22P02")] {
        let response = query(&fixture, "SELECT $1::bytea", &[25], false, &[Some(input)]).await;
        assert_eq!(response.errors, [error], "{response:?}");
        assert_eq!(response.parameters, [25], "{response:?}");
        assert_eq!(response.columns, vec![vec![17], vec![17]], "{response:?}");
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
        assert_eq!(response.errors, ["42883"], "{response:?}");
        assert!(!response.tags.contains(&b'1'), "{response:?}");
        assert!(response.rows.is_empty(), "{response:?}");
    }
    for oids in [&[17, 25][..], &[25, 17][..], &[17][..]] {
        let response = query(
            &fixture,
            "SELECT strpos($1,$2)",
            oids,
            false,
            &[Some(br"\x01"), Some(b"a")],
        )
        .await;
        assert_eq!(response.errors, ["42883"], "{response:?}");
        assert!(!response.tags.contains(&b'1'), "{response:?}");
        assert!(response.rows.is_empty(), "{response:?}");
    }
    let response = query(
        &fixture,
        "SELECT strpos($1::bytea, 'a')",
        &[],
        false,
        &[Some(br"\x01")],
    )
    .await;
    assert_eq!(response.errors, ["42883"], "{response:?}");
    assert!(!response.tags.contains(&b'1'), "{response:?}");
    assert!(response.rows.is_empty(), "{response:?}");
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

#[tokio::test]
async fn short_binary_integer_parameters_are_protocol_errors() {
    let fixture = Fixture::new().await;
    for (sql, oid, length, code) in [
        ("SELECT $1::int4", 23, 1, "08P01"),
        ("SELECT $1::int4", 23, 0, "08P01"),
        ("SELECT $1::int4", 23, 5, "22P03"),
        ("SELECT $1::int2", 21, 1, "08P01"),
        ("SELECT $1::int2", 21, 3, "22P03"),
        ("SELECT $1::int8", 20, 7, "08P01"),
        ("SELECT $1::int8", 20, 9, "22P03"),
        ("SELECT $1::bool", 16, 0, "08P01"),
        ("SELECT $1::bool", 16, 2, "22P03"),
    ] {
        let bytes = vec![0; length];
        let response = query(&fixture, sql, &[oid], true, &[Some(&bytes)]).await;
        assert_eq!(response.errors, [code], "{response:?}");
        assert!(response.tags.contains(&b'1'), "{response:?}");
        assert!(!response.tags.contains(&b'2'), "{response:?}");
        assert!(response.rows.is_empty(), "{response:?}");
    }
    fixture.stop().await;
}

#[tokio::test]
async fn declared_integer_narrowing_cast_out_of_range_is_22003() {
    // PG18.6 reports 22003 at Bind (custom-plan constant folding); GigglesDB evaluates parameter-dependent casts at Execute — same SQLSTATE, documented phase deviation.
    let fixture = Fixture::new().await;
    for (sql, oid, value) in [
        ("SELECT $1::int4", 20, &b"2147483648"[..]),
        ("SELECT $1::int2", 20, &b"40000"[..]),
        ("SELECT $1::int2", 23, &b"40000"[..]),
        ("SELECT $1::int4+0", 20, &b"2147483648"[..]),
    ] {
        let response = query(&fixture, sql, &[oid], false, &[Some(value)]).await;
        assert_eq!(response.errors, ["22003"], "{response:?}");
        assert!(response.tags.contains(&b'1'), "{response:?}");
        assert!(response.tags.contains(&b'2'), "{response:?}");
        assert!(response.rows.is_empty(), "{response:?}");
    }
    let binary = 2147483648i64.to_be_bytes();
    let response = query(&fixture, "SELECT $1::int4", &[20], true, &[Some(&binary)]).await;
    assert_eq!(response.errors, ["22003"], "{response:?}");
    assert!(response.tags.contains(&b'1'), "{response:?}");
    assert!(response.tags.contains(&b'2'), "{response:?}");
    assert!(response.rows.is_empty(), "{response:?}");
    let response = query(
        &fixture,
        "SELECT $1::int4",
        &[20],
        false,
        &[Some(b"2147483647")],
    )
    .await;
    response.success(&[20], 23, Some(b"2147483647"));
    fixture.stop().await;
}

#[tokio::test]
async fn owner_catalog_declared_bytea_retry_preserves_transaction() {
    let f = Fixture::new().await;
    let mut stream = f.connect().await;
    send(&mut stream, b'Q', b"BEGIN\0").await;
    assert_eq!(read(&mut stream).await, (b'C', b"BEGIN\0".to_vec()));
    assert_eq!(read(&mut stream).await, (b'Z', b"T".to_vec()));

    let mut parse = vec![0];
    cstr("SELECT $1", &mut parse);
    parse.extend(1u16.to_be_bytes());
    parse.extend(17u32.to_be_bytes());
    send(&mut stream, b'P', &parse).await;
    send(&mut stream, b'S', &[]).await;
    assert_eq!(read(&mut stream).await, (b'1', vec![]));
    assert_eq!(read(&mut stream).await, (b'Z', b"T".to_vec()));

    send(&mut stream, b'Q', b"SELECT 1\0").await;
    assert_eq!(read(&mut stream).await.0, b'T');
    assert_eq!(
        read(&mut stream).await,
        (b'D', vec![0, 1, 0, 0, 0, 1, b'1'])
    );
    assert_eq!(read(&mut stream).await, (b'C', b"SELECT 1\0".to_vec()));
    assert_eq!(read(&mut stream).await, (b'Z', b"T".to_vec()));
    send(&mut stream, b'Q', b"COMMIT\0").await;
    assert_eq!(read(&mut stream).await, (b'C', b"COMMIT\0".to_vec()));
    assert_eq!(read(&mut stream).await, (b'Z', b"I".to_vec()));
    drop(stream);
    f.stop().await;
}

#[tokio::test]
async fn declared_wider_integer_comparison_against_narrow_column_matches_pg18() {
    let fixture = Fixture::new().await;
    let mut stream = fixture.connect().await;
    for sql in [
        "CREATE TABLE t(id integer primary key, s smallint, n integer)",
        "INSERT INTO t VALUES (1, 1, 1), (2, 2, 2)",
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
    drop(stream);
    for (sql, oid, value, ids) in [
        (
            "SELECT id FROM t WHERE s = $1 ORDER BY id",
            20,
            &b"100000"[..],
            &[][..],
        ),
        (
            "SELECT id FROM t WHERE n < $1 ORDER BY id",
            20,
            &b"1099511627776"[..],
            &["1", "2"][..],
        ),
        (
            "SELECT id FROM t WHERE n = $1 ORDER BY id",
            20,
            &b"1099511627776"[..],
            &[][..],
        ),
        (
            "SELECT id FROM t WHERE s < $1 ORDER BY id",
            23,
            &b"100000"[..],
            &["1", "2"][..],
        ),
    ] {
        let response = query(&fixture, sql, &[oid], false, &[Some(value)]).await;
        assert!(response.errors.is_empty(), "{response:?}");
        assert!(response.tags.contains(&b'1'), "{response:?}");
        assert!(response.tags.contains(&b'2'), "{response:?}");
        assert_eq!(response.parameters, [oid], "{response:?}");
        assert_eq!(response.columns, vec![vec![23], vec![23]], "{response:?}");
        let rows: Vec<_> = ids
            .iter()
            .map(|id| vec![Some(id.as_bytes().to_vec())])
            .collect();
        assert_eq!(response.rows, rows, "{response:?}");
        assert_eq!(
            response.commands,
            [format!("SELECT {}", ids.len())],
            "{response:?}"
        );
    }
    fixture.stop().await;
}

#[tokio::test]
async fn declared_integer_parameters_match_pg18_probe_table() {
    let fixture = Fixture::new().await;
    for sql in [
        "CREATE TABLE t(id integer primary key, s smallint, n integer, b bigint)",
        "INSERT INTO t VALUES (1, 1, 1, 1), (2, 2, 2, 2)",
        "CREATE TABLE t2(id integer primary key, num numeric)",
    ] {
        let response = query(&fixture, sql, &[], false, &[]).await;
        assert!(response.errors.is_empty(), "{response:?}");
    }

    struct Case {
        sql: &'static str,
        oids: &'static [u32],
        values: &'static [&'static str],
        column: Option<u32>,
        rows: &'static [&'static str],
        command: &'static str,
    }
    // An empty command denotes 22003 at Execute: Parse and Bind both complete.
    let cases = [
        Case {
            sql: "SELECT id FROM t WHERE $1 = n ORDER BY id",
            oids: &[20],
            values: &["1099511627776"],
            column: Some(23),
            rows: &[],
            command: "SELECT 0",
        },
        Case {
            sql: "SELECT id FROM t WHERE id = $1",
            oids: &[20],
            values: &["1"],
            column: Some(23),
            rows: &["1"],
            command: "SELECT 1",
        },
        Case {
            sql: "SELECT id FROM t WHERE id = $1",
            oids: &[20],
            values: &["1099511627776"],
            column: Some(23),
            rows: &[],
            command: "SELECT 0",
        },
        Case {
            sql: "SELECT id FROM t WHERE n IN ($1,$2) ORDER BY id",
            oids: &[20, 20],
            values: &["1099511627776", "2"],
            column: Some(23),
            rows: &["2"],
            command: "SELECT 1",
        },
        Case {
            sql: "DELETE FROM t WHERE n = $1",
            oids: &[20],
            values: &["1099511627776"],
            column: None,
            rows: &[],
            command: "DELETE 0",
        },
        Case {
            sql: "SELECT $1 + 1",
            oids: &[20],
            values: &["5"],
            column: Some(20),
            rows: &["6"],
            command: "SELECT 1",
        },
        Case {
            sql: "SELECT $1 + 1",
            oids: &[21],
            values: &["5"],
            column: Some(23),
            rows: &["6"],
            command: "SELECT 1",
        },
        Case {
            sql: "SELECT $1 + $2",
            oids: &[21, 21],
            values: &["5", "6"],
            column: Some(21),
            rows: &["11"],
            command: "SELECT 1",
        },
        Case {
            sql: "SELECT $1 + $2",
            oids: &[21, 21],
            values: &["20000", "20000"],
            column: Some(21),
            rows: &[],
            command: "",
        },
        Case {
            sql: "SELECT $1 + $2",
            oids: &[23, 20],
            values: &["5", "6"],
            column: Some(20),
            rows: &["11"],
            command: "SELECT 1",
        },
        Case {
            sql: "SELECT $1 * 2",
            oids: &[21],
            values: &["20000"],
            column: Some(23),
            rows: &["40000"],
            command: "SELECT 1",
        },
        Case {
            sql: "SELECT $1",
            oids: &[20],
            values: &["5"],
            column: Some(20),
            rows: &["5"],
            command: "SELECT 1",
        },
        Case {
            sql: "SELECT $1",
            oids: &[23],
            values: &["5"],
            column: Some(23),
            rows: &["5"],
            command: "SELECT 1",
        },
        Case {
            sql: "SELECT s + $1 FROM t WHERE id = 1",
            oids: &[21],
            values: &["32767"],
            column: Some(21),
            rows: &[],
            command: "",
        },
        Case {
            sql: "INSERT INTO t VALUES ($1, 1, 1, 1)",
            oids: &[20],
            values: &["2147483648"],
            column: None,
            rows: &[],
            command: "",
        },
        Case {
            sql: "INSERT INTO t(id,s) VALUES (3,$1)",
            oids: &[20],
            values: &["40000"],
            column: None,
            rows: &[],
            command: "",
        },
        Case {
            sql: "UPDATE t SET n = $1 WHERE id = 1",
            oids: &[20],
            values: &["1099511627776"],
            column: None,
            rows: &[],
            command: "",
        },
        Case {
            sql: "UPDATE t SET s = $1 WHERE id = $2",
            oids: &[23, 23],
            values: &["40000", "1"],
            column: None,
            rows: &[],
            command: "",
        },
        Case {
            sql: "UPDATE t SET n = $1 + 1 WHERE id = 1",
            oids: &[20],
            values: &["1099511627776"],
            column: None,
            rows: &[],
            command: "",
        },
        Case {
            sql: "INSERT INTO t(id,n) VALUES (6,$1),(7,$2)",
            oids: &[20, 20],
            values: &["1", "1099511627776"],
            column: None,
            rows: &[],
            command: "",
        },
        Case {
            sql: "SELECT id FROM t WHERE id = 6",
            oids: &[],
            values: &[],
            column: Some(23),
            rows: &[],
            command: "SELECT 0",
        },
        Case {
            sql: "INSERT INTO t(id,s) VALUES (5,$1)",
            oids: &[20],
            values: &["7"],
            column: None,
            rows: &[],
            command: "INSERT 0 1",
        },
        Case {
            sql: "SELECT s FROM t WHERE id = 5",
            oids: &[],
            values: &[],
            column: Some(21),
            rows: &["7"],
            command: "SELECT 1",
        },
        Case {
            sql: "INSERT INTO t2(id,num) VALUES (3,$1)",
            oids: &[20],
            values: &["9"],
            column: None,
            rows: &[],
            command: "INSERT 0 1",
        },
        Case {
            sql: "SELECT num FROM t2 WHERE id = 3",
            oids: &[],
            values: &[],
            column: Some(1700),
            rows: &["9"],
            command: "SELECT 1",
        },
    ];
    for case in cases {
        let values: Vec<_> = case
            .values
            .iter()
            .map(|value| Some(value.as_bytes()))
            .collect();
        let response = query(&fixture, case.sql, case.oids, false, &values).await;
        assert!(response.tags.contains(&b'1'), "{response:?}");
        assert!(response.tags.contains(&b'2'), "{response:?}");
        assert_eq!(response.parameters, case.oids, "{response:?}");
        let columns = case
            .column
            .map_or_else(Vec::new, |oid| vec![vec![oid], vec![oid]]);
        assert_eq!(response.columns, columns, "{response:?}");
        let rows: Vec<_> = case
            .rows
            .iter()
            .map(|value| vec![Some(value.as_bytes().to_vec())])
            .collect();
        assert_eq!(response.rows, rows, "{response:?}");
        if case.command.is_empty() {
            assert_eq!(response.errors, ["22003"], "{response:?}");
            assert!(response.commands.is_empty(), "{response:?}");
        } else {
            assert!(response.errors.is_empty(), "{response:?}");
            assert_eq!(response.commands, [case.command], "{response:?}");
        }
    }
    fixture.stop().await;
}
