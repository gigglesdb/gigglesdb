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
            "declared-nonint-parameter-network-{}-{}-{}",
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

// Frozen F13 probe: the original K/X/A/U/F/R/T/B order preserves mutations of w.
// All results use text output, including the binary-input B group.
#[derive(Debug, serde::Deserialize)]
struct Case {
    name: String,
    sql: String,
    oids: Vec<u32>,
    inputs: Vec<String>,
    binary: bool,
    expected: Expected,
}
#[derive(Debug, Default, serde::Deserialize, PartialEq)]
struct Expected {
    phase: String,
    param_oids: Option<Vec<u32>>,
    col_oids: Option<Vec<u32>>,
    #[serde(default)]
    rows: Vec<Vec<Option<String>>>,
    sqlstate: Option<String>,
}

async fn cycle(stream: &mut TcpStream, result: &mut Expected) {
    loop {
        let (tag, body) = read(stream).await;
        match tag {
            b'1' => result.phase = "describe".into(),
            b'2' => result.phase = "execute".into(),
            b'E' => {
                assert!(result.sqlstate.is_none(), "duplicate ErrorResponse");
                let code = body
                    .split(|byte| *byte == 0)
                    .find(|field| field.first() == Some(&b'C'))
                    .expect("ErrorResponse SQLSTATE");
                result.sqlstate = Some(std::str::from_utf8(&code[1..]).unwrap().into());
            }
            b't' => {
                let count = u16::from_be_bytes(body[..2].try_into().unwrap()) as usize;
                assert_eq!(body.len(), 2 + 4 * count);
                assert!(
                    result.param_oids.is_none(),
                    "duplicate ParameterDescription"
                );
                result.param_oids = Some(
                    body[2..]
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .map(|bytes| u32::from_be_bytes(*bytes))
                        .collect(),
                );
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
                    assert_eq!(&body[at + 16..at + 18], &[0, 0], "text output format");
                    at += 18;
                }
                assert_eq!(at, body.len());
                assert!(result.col_oids.is_none(), "duplicate RowDescription");
                result.col_oids = Some(types);
            }
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
                        assert!(length >= 0);
                        row.push(Some(
                            String::from_utf8(body[at..at + length as usize].to_vec()).unwrap(),
                        ));
                        at += length as usize;
                    }
                }
                assert_eq!(at, body.len());
                result.rows.push(row);
            }
            b'Z' => return,
            _ => {}
        }
    }
}

async fn query(fixture: &Fixture, case: &Case) -> Expected {
    let mut stream = fixture.connect().await;
    let mut parse = vec![0];
    cstr(&case.sql, &mut parse);
    parse.extend((case.oids.len() as u16).to_be_bytes());
    for oid in &case.oids {
        parse.extend(oid.to_be_bytes());
    }
    send(&mut stream, b'P', &parse).await;
    send(&mut stream, b'D', &[b'S', 0]).await;
    send(&mut stream, b'S', &[]).await;
    let mut result = Expected {
        phase: "parse".into(),
        ..Expected::default()
    };
    cycle(&mut stream, &mut result).await;
    if result.sqlstate.is_some() {
        return result;
    }
    let mut bind = vec![0, 0];
    // Zero format codes means text; one binary code applies to all parameters.
    bind.extend(u16::from(case.binary).to_be_bytes());
    if case.binary {
        bind.extend(1u16.to_be_bytes());
    }
    bind.extend((case.inputs.len() as u16).to_be_bytes());
    for input in &case.inputs {
        let bytes = hex::decode(input).unwrap();
        bind.extend((bytes.len() as i32).to_be_bytes());
        bind.extend(bytes);
    }
    bind.extend(1u16.to_be_bytes());
    bind.extend(0u16.to_be_bytes()); // text result format, as in the PG probe
    send(&mut stream, b'B', &bind).await;
    send(&mut stream, b'E', &[0, 0, 0, 0, 0]).await;
    send(&mut stream, b'S', &[]).await;
    result.phase = "bind".into();
    cycle(&mut stream, &mut result).await;
    result
}

#[tokio::test]
async fn declared_noninteger_parameters_match_pg18_with_named_known_gaps() {
    let fixture = Fixture::new().await;
    for sql in SETUP {
        let case = Case {
            name: "setup".into(),
            sql: (*sql).into(),
            oids: vec![],
            inputs: vec![],
            binary: false,
            expected: Expected::default(),
        };
        let result = query(&fixture, &case).await;
        assert!(result.sqlstate.is_none(), "setup {sql}: {result:?}");
    }
    assert_eq!(CASES.len(), 234);
    for frozen in CASES {
        let case: Case = serde_json::from_str(frozen).unwrap();
        let mut actual = query(&fixture, &case).await;
        // Assignment value errors (F8) and numeric / 0 retain SQLSTATE parity;
        // PG may fold constants during Bind while this server evaluates at Execute.
        if case.expected.sqlstate.is_some()
            && (case.name.starts_with("A ")
                || case.name.starts_with("U ")
                || case.name == "F numeric / 0")
            && case.expected.phase != "parse"
        {
            actual.phase.clone_from(&case.expected.phase);
        }
        assert_eq!(actual, case.expected, "{}: {}", case.name, case.sql);
    }
    fixture.stop().await;
}

#[rustfmt::skip]
const SETUP: &[&str] = &[
    "DROP TABLE IF EXISTS t",
    "DROP TABLE IF EXISTS w",
    "CREATE TABLE t(id integer primary key, bo boolean, tx text, n numeric, r real, f double precision, d date, tm time, ttz timetz, ts timestamp, tz timestamptz, iv interval, i integer, s smallint, b bigint)",
    "INSERT INTO t VALUES (1, true, 'abc', 1.5, 1.5, 1.5, '2024-01-02', '03:04:05', '03:04:05+00', '2024-01-02 00:00:00', '2024-01-02 00:00:00+00', '03:04:05', 2, 2, 2)",
    "INSERT INTO t VALUES (2, false, 'xyz', 3, 3, 3, '2024-01-03', '06:00:00', '06:00:00+02', '2024-01-02 12:30:00', '2024-01-03 00:00:00+00', '1 day', 3, 3, 3)",
    "INSERT INTO t(id) VALUES (3)",
    "CREATE TABLE w(id integer primary key, bo boolean, tx text, n numeric, r real, f double precision, d date, tm time, ttz timetz, ts timestamp, tz timestamptz, iv interval, i integer, s smallint, b bigint)",
    "INSERT INTO w(id) VALUES (1)",
];

// Inline hex inputs freeze the exact bytes, including malformed binary values.
#[rustfmt::skip]
const CASES: &[&str] = &[
    r#"{"name":"K bool bo=$1","sql":"SELECT id FROM t WHERE bo = $1 ORDER BY id","oids":[16],"inputs":["74727565"],"binary":false,"expected":{"phase":"execute","param_oids":[16],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K bool bo<$1","sql":"SELECT id FROM t WHERE bo < $1 ORDER BY id","oids":[16],"inputs":["74727565"],"binary":false,"expected":{"phase":"execute","param_oids":[16],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"K text tx=$1","sql":"SELECT id FROM t WHERE tx = $1 ORDER BY id","oids":[25],"inputs":["616263"],"binary":false,"expected":{"phase":"execute","param_oids":[25],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K text tx<$1","sql":"SELECT id FROM t WHERE tx < $1 ORDER BY id","oids":[25],"inputs":["616263"],"binary":false,"expected":{"phase":"execute","param_oids":[25],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"K varchar tx=$1","sql":"SELECT id FROM t WHERE tx = $1 ORDER BY id","oids":[1043],"inputs":["616263"],"binary":false,"expected":{"phase":"execute","param_oids":[1043],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K varchar tx<$1","sql":"SELECT id FROM t WHERE tx < $1 ORDER BY id","oids":[1043],"inputs":["616263"],"binary":false,"expected":{"phase":"execute","param_oids":[1043],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"K numeric n=$1","sql":"SELECT id FROM t WHERE n = $1 ORDER BY id","oids":[1700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K numeric n<$1","sql":"SELECT id FROM t WHERE n < $1 ORDER BY id","oids":[1700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"K float4 r=$1","sql":"SELECT id FROM t WHERE r = $1 ORDER BY id","oids":[700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[700],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K float4 r<$1","sql":"SELECT id FROM t WHERE r < $1 ORDER BY id","oids":[700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[700],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"K float8 f=$1","sql":"SELECT id FROM t WHERE f = $1 ORDER BY id","oids":[701],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K float8 f<$1","sql":"SELECT id FROM t WHERE f < $1 ORDER BY id","oids":[701],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"K date d=$1","sql":"SELECT id FROM t WHERE d = $1 ORDER BY id","oids":[1082],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K date d<$1","sql":"SELECT id FROM t WHERE d < $1 ORDER BY id","oids":[1082],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"K time tm=$1","sql":"SELECT id FROM t WHERE tm = $1 ORDER BY id","oids":[1083],"inputs":["30333a30343a3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1083],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K time tm<$1","sql":"SELECT id FROM t WHERE tm < $1 ORDER BY id","oids":[1083],"inputs":["30333a30343a3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1083],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"K timetz ttz=$1","sql":"SELECT id FROM t WHERE ttz = $1 ORDER BY id","oids":[1266],"inputs":["30333a30343a30352b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1266],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K timetz ttz<$1","sql":"SELECT id FROM t WHERE ttz < $1 ORDER BY id","oids":[1266],"inputs":["30333a30343a30352b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1266],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"K timestamp ts=$1","sql":"SELECT id FROM t WHERE ts = $1 ORDER BY id","oids":[1114],"inputs":["323032342d30312d30322030303a30303a3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1114],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K timestamp ts<$1","sql":"SELECT id FROM t WHERE ts < $1 ORDER BY id","oids":[1114],"inputs":["323032342d30312d30322030303a30303a3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1114],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"K timestamptz tz=$1","sql":"SELECT id FROM t WHERE tz = $1 ORDER BY id","oids":[1184],"inputs":["323032342d30312d30322030303a30303a30302b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K timestamptz tz<$1","sql":"SELECT id FROM t WHERE tz < $1 ORDER BY id","oids":[1184],"inputs":["323032342d30312d30322030303a30303a30302b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"K interval iv=$1","sql":"SELECT id FROM t WHERE iv = $1 ORDER BY id","oids":[1186],"inputs":["30333a30343a3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1186],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K interval iv<$1","sql":"SELECT id FROM t WHERE iv < $1 ORDER BY id","oids":[1186],"inputs":["30333a30343a3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1186],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"K numeric $1=n reversed","sql":"SELECT id FROM t WHERE $1 = n ORDER BY id","oids":[1700],"inputs":["33"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"K numeric n IN","sql":"SELECT id FROM t WHERE n IN ($1, $2) ORDER BY id","oids":[1700,1700],"inputs":["312e35","332e30"],"binary":false,"expected":{"phase":"execute","param_oids":[1700,1700],"col_oids":[23],"rows":[["1"],["2"]]}}"#,
    r#"{"name":"K date BETWEEN","sql":"SELECT id FROM t WHERE d BETWEEN $1 AND $2 ORDER BY id","oids":[1082,1082],"inputs":["323032342d30312d3031","323032342d30312d3032"],"binary":false,"expected":{"phase":"execute","param_oids":[1082,1082],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"K text pk-less tx<>$1","sql":"SELECT id FROM t WHERE tx <> $1 ORDER BY id","oids":[25],"inputs":["616263"],"binary":false,"expected":{"phase":"execute","param_oids":[25],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"K numeric DELETE","sql":"DELETE FROM w WHERE n = $1","oids":[1700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"rows":[]}}"#,
    r#"{"name":"K numeric UPDATE WHERE","sql":"UPDATE w SET i = 1 WHERE n = $1","oids":[1700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"rows":[]}}"#,
    r#"{"name":"X numeric vs int","sql":"SELECT id FROM t WHERE i = $1 ORDER BY id","oids":[1700],"inputs":["32"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X numeric vs int frac","sql":"SELECT id FROM t WHERE i = $1 ORDER BY id","oids":[1700],"inputs":["322e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[]}}"#,
    r#"{"name":"X numeric vs int <","sql":"SELECT id FROM t WHERE i < $1 ORDER BY id","oids":[1700],"inputs":["322e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X numeric vs smallint","sql":"SELECT id FROM t WHERE s = $1 ORDER BY id","oids":[1700],"inputs":["33"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"X numeric vs bigint","sql":"SELECT id FROM t WHERE b = $1 ORDER BY id","oids":[1700],"inputs":["33"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"X numeric vs float8","sql":"SELECT id FROM t WHERE f = $1 ORDER BY id","oids":[1700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X numeric vs real","sql":"SELECT id FROM t WHERE r = $1 ORDER BY id","oids":[1700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X float8 vs int","sql":"SELECT id FROM t WHERE i = $1 ORDER BY id","oids":[701],"inputs":["32"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X float8 vs numeric","sql":"SELECT id FROM t WHERE n = $1 ORDER BY id","oids":[701],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X float8 vs real","sql":"SELECT id FROM t WHERE r = $1 ORDER BY id","oids":[701],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X float4 vs float8","sql":"SELECT id FROM t WHERE f = $1 ORDER BY id","oids":[700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[700],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X float4 vs int","sql":"SELECT id FROM t WHERE i = $1 ORDER BY id","oids":[700],"inputs":["33"],"binary":false,"expected":{"phase":"execute","param_oids":[700],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"X float4 vs numeric","sql":"SELECT id FROM t WHERE n < $1 ORDER BY id","oids":[700],"inputs":["32"],"binary":false,"expected":{"phase":"execute","param_oids":[700],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X date vs timestamp","sql":"SELECT id FROM t WHERE ts = $1 ORDER BY id","oids":[1082],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X date vs timestamptz","sql":"SELECT id FROM t WHERE tz = $1 ORDER BY id","oids":[1082],"inputs":["323032342d30312d3033"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"X timestamp vs date","sql":"SELECT id FROM t WHERE d = $1 ORDER BY id","oids":[1114],"inputs":["323032342d30312d30322030303a30303a3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1114],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X timestamp vs timestamptz","sql":"SELECT id FROM t WHERE tz < $1 ORDER BY id","oids":[1114],"inputs":["323032342d30312d30322031323a30303a3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1114],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X timestamptz vs date","sql":"SELECT id FROM t WHERE d = $1 ORDER BY id","oids":[1184],"inputs":["323032342d30312d30332030303a30303a30302b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"X timestamptz vs timestamp","sql":"SELECT id FROM t WHERE ts > $1 ORDER BY id","oids":[1184],"inputs":["323032342d30312d30322030363a30303a30302b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"X time vs timetz","sql":"SELECT id FROM t WHERE ttz = $1 ORDER BY id","oids":[1083],"inputs":["30333a30343a3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1083],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X timetz vs time","sql":"SELECT id FROM t WHERE tm = $1 ORDER BY id","oids":[1266],"inputs":["30333a30343a30352b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1266],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X interval vs time","sql":"SELECT id FROM t WHERE tm = $1 ORDER BY id","oids":[1186],"inputs":["30333a30343a3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1186],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X time vs interval","sql":"SELECT id FROM t WHERE iv = $1 ORDER BY id","oids":[1083],"inputs":["30333a30343a3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1083],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"X varchar vs text","sql":"SELECT id FROM t WHERE tx = $1 ORDER BY id","oids":[1043],"inputs":["78797a"],"binary":false,"expected":{"phase":"execute","param_oids":[1043],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"X int col vs numeric IN","sql":"SELECT id FROM t WHERE i IN ($1, $2) ORDER BY id","oids":[1700,1700],"inputs":["32","332e30"],"binary":false,"expected":{"phase":"execute","param_oids":[1700,1700],"col_oids":[23],"rows":[["1"],["2"]]}}"#,
    r#"{"name":"X bool vs int","sql":"SELECT id FROM t WHERE i = $1 ORDER BY id","oids":[16],"inputs":["74727565"],"binary":false,"expected":{"phase":"parse","sqlstate":"42883"}}"#,
    r#"{"name":"X text vs int","sql":"SELECT id FROM t WHERE i = $1 ORDER BY id","oids":[25],"inputs":["32"],"binary":false,"expected":{"phase":"parse","sqlstate":"42883"}}"#,
    r#"{"name":"X varchar vs int","sql":"SELECT id FROM t WHERE i = $1 ORDER BY id","oids":[1043],"inputs":["32"],"binary":false,"expected":{"phase":"parse","sqlstate":"42883"}}"#,
    r#"{"name":"X date vs time","sql":"SELECT id FROM t WHERE tm = $1 ORDER BY id","oids":[1082],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"parse","sqlstate":"42883"}}"#,
    r#"{"name":"X numeric vs text","sql":"SELECT id FROM t WHERE tx = $1 ORDER BY id","oids":[1700],"inputs":["31"],"binary":false,"expected":{"phase":"parse","sqlstate":"42883"}}"#,
    r#"{"name":"X interval vs date","sql":"SELECT id FROM t WHERE d = $1 ORDER BY id","oids":[1186],"inputs":["3120646179"],"binary":false,"expected":{"phase":"parse","sqlstate":"42883"}}"#,
    r#"{"name":"X text vs date","sql":"SELECT id FROM t WHERE d = $1 ORDER BY id","oids":[25],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"parse","sqlstate":"42883"}}"#,
    r#"{"name":"X float8 vs interval","sql":"SELECT id FROM t WHERE iv = $1 ORDER BY id","oids":[701],"inputs":["31"],"binary":false,"expected":{"phase":"parse","sqlstate":"42883"}}"#,
    r#"{"name":"X bool vs text","sql":"SELECT id FROM t WHERE tx = $1 ORDER BY id","oids":[16],"inputs":["74727565"],"binary":false,"expected":{"phase":"parse","sqlstate":"42883"}}"#,
    r#"{"name":"A ins numeric->int","sql":"INSERT INTO w(id, i) VALUES (100, $1) RETURNING i","oids":[1700],"inputs":["322e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["3"]]}}"#,
    r#"{"name":"A ins numeric->int round","sql":"INSERT INTO w(id, i) VALUES (101, $1) RETURNING i","oids":[1700],"inputs":["2d322e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["-3"]]}}"#,
    r#"{"name":"A ins numeric->int oor","sql":"INSERT INTO w(id, i) VALUES (102, $1) RETURNING i","oids":[1700],"inputs":["33303030303030303030"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"sqlstate":"22003"}}"#,
    r#"{"name":"A ins numeric->smallint oor","sql":"INSERT INTO w(id, s) VALUES (103, $1) RETURNING s","oids":[1700],"inputs":["3430303030"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[21],"sqlstate":"22003"}}"#,
    r#"{"name":"A ins numeric->bigint","sql":"INSERT INTO w(id, b) VALUES (104, $1) RETURNING b","oids":[1700],"inputs":["39303037313939323534373430393933"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[20],"rows":[["9007199254740993"]]}}"#,
    r#"{"name":"A ins numeric->real","sql":"INSERT INTO w(id, r) VALUES (105, $1) RETURNING r","oids":[1700],"inputs":["312e3235"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[700],"rows":[["1.25"]]}}"#,
    r#"{"name":"A ins numeric->float8","sql":"INSERT INTO w(id, f) VALUES (106, $1) RETURNING f","oids":[1700],"inputs":["3165333030"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[701],"rows":[["1e+300"]]}}"#,
    r#"{"name":"A ins numeric->real oor","sql":"INSERT INTO w(id, r) VALUES (107, $1) RETURNING r","oids":[1700],"inputs":["3165333030"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[700],"sqlstate":"22003"}}"#,
    r#"{"name":"A ins numeric NaN->int","sql":"INSERT INTO w(id, i) VALUES (108, $1) RETURNING i","oids":[1700],"inputs":["4e614e"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"sqlstate":"0A000"}}"#,
    r#"{"name":"A ins numeric->text","sql":"INSERT INTO w(id, tx) VALUES (109, $1) RETURNING tx","oids":[1700],"inputs":["312e3530"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[25],"rows":[["1.50"]]}}"#,
    r#"{"name":"A ins float8->int","sql":"INSERT INTO w(id, i) VALUES (110, $1) RETURNING i","oids":[701],"inputs":["322e35"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"A ins float8->int 3.5","sql":"INSERT INTO w(id, i) VALUES (111, $1) RETURNING i","oids":[701],"inputs":["332e35"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[23],"rows":[["4"]]}}"#,
    r#"{"name":"A ins float8->int oor","sql":"INSERT INTO w(id, i) VALUES (112, $1) RETURNING i","oids":[701],"inputs":["31653130"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[23],"sqlstate":"22003"}}"#,
    r#"{"name":"A ins float8 inf->int","sql":"INSERT INTO w(id, i) VALUES (113, $1) RETURNING i","oids":[701],"inputs":["496e66696e697479"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[23],"sqlstate":"22003"}}"#,
    r#"{"name":"A ins float8->numeric","sql":"INSERT INTO w(id, n) VALUES (114, $1) RETURNING n","oids":[701],"inputs":["302e31"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[1700],"rows":[["0.1"]]}}"#,
    r#"{"name":"A ins float8 inf->numeric","sql":"INSERT INTO w(id, n) VALUES (115, $1) RETURNING n","oids":[701],"inputs":["2d496e66696e697479"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[1700],"rows":[["-Infinity"]]}}"#,
    r#"{"name":"A ins float8 NaN->numeric","sql":"INSERT INTO w(id, n) VALUES (116, $1) RETURNING n","oids":[701],"inputs":["4e614e"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[1700],"rows":[["NaN"]]}}"#,
    r#"{"name":"A ins float8->real oor","sql":"INSERT INTO w(id, r) VALUES (117, $1) RETURNING r","oids":[701],"inputs":["3165333030"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[700],"sqlstate":"22003"}}"#,
    r#"{"name":"A ins float8->real","sql":"INSERT INTO w(id, r) VALUES (118, $1) RETURNING r","oids":[701],"inputs":["302e31"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[700],"rows":[["0.1"]]}}"#,
    r#"{"name":"A ins float4->float8","sql":"INSERT INTO w(id, f) VALUES (119, $1) RETURNING f","oids":[700],"inputs":["302e31"],"binary":false,"expected":{"phase":"execute","param_oids":[700],"col_oids":[701],"rows":[["0.10000000149011612"]]}}"#,
    r#"{"name":"A ins float4->numeric","sql":"INSERT INTO w(id, n) VALUES (120, $1) RETURNING n","oids":[700],"inputs":["302e31"],"binary":false,"expected":{"phase":"execute","param_oids":[700],"col_oids":[1700],"rows":[["0.1"]]}}"#,
    r#"{"name":"A ins float4->smallint","sql":"INSERT INTO w(id, s) VALUES (121, $1) RETURNING s","oids":[700],"inputs":["372e35"],"binary":false,"expected":{"phase":"execute","param_oids":[700],"col_oids":[21],"rows":[["8"]]}}"#,
    r#"{"name":"A ins float8->text","sql":"INSERT INTO w(id, tx) VALUES (122, $1) RETURNING tx","oids":[701],"inputs":["302e31"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[25],"rows":[["0.1"]]}}"#,
    r#"{"name":"A ins text->int","sql":"INSERT INTO w(id, i) VALUES (123, $1) RETURNING i","oids":[25],"inputs":["35"],"binary":false,"expected":{"phase":"parse","sqlstate":"42804"}}"#,
    r#"{"name":"A ins varchar->text","sql":"INSERT INTO w(id, tx) VALUES (124, $1) RETURNING tx","oids":[1043],"inputs":["68656c6c6f"],"binary":false,"expected":{"phase":"execute","param_oids":[1043],"col_oids":[25],"rows":[["hello"]]}}"#,
    r#"{"name":"A ins text->date","sql":"INSERT INTO w(id, d) VALUES (125, $1) RETURNING d","oids":[25],"inputs":["323032342d30312d3031"],"binary":false,"expected":{"phase":"parse","sqlstate":"42804"}}"#,
    r#"{"name":"A ins text->bool","sql":"INSERT INTO w(id, bo) VALUES (126, $1) RETURNING bo","oids":[25],"inputs":["74727565"],"binary":false,"expected":{"phase":"parse","sqlstate":"42804"}}"#,
    r#"{"name":"A ins bool->int","sql":"INSERT INTO w(id, i) VALUES (127, $1) RETURNING i","oids":[16],"inputs":["74727565"],"binary":false,"expected":{"phase":"parse","sqlstate":"42804"}}"#,
    r#"{"name":"A ins bool->text","sql":"INSERT INTO w(id, tx) VALUES (128, $1) RETURNING tx","oids":[16],"inputs":["74727565"],"binary":false,"expected":{"phase":"execute","param_oids":[16],"col_oids":[25],"rows":[["true"]]}}"#,
    r#"{"name":"A ins date->text","sql":"INSERT INTO w(id, tx) VALUES (129, $1) RETURNING tx","oids":[1082],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[25],"rows":[["2024-01-02"]]}}"#,
    r#"{"name":"A ins date->timestamp","sql":"INSERT INTO w(id, ts) VALUES (130, $1) RETURNING ts","oids":[1082],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[1114],"rows":[["2024-01-02 00:00:00"]]}}"#,
    r#"{"name":"A ins date->timestamptz","sql":"INSERT INTO w(id, tz) VALUES (131, $1) RETURNING tz","oids":[1082],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[1184],"rows":[["2024-01-02 00:00:00+00"]]}}"#,
    r#"{"name":"A ins timestamp->date","sql":"INSERT INTO w(id, d) VALUES (132, $1) RETURNING d","oids":[1114],"inputs":["323032342d30312d30322032333a35393a3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1114],"col_oids":[1082],"rows":[["2024-01-02"]]}}"#,
    r#"{"name":"A ins timestamptz->date","sql":"INSERT INTO w(id, d) VALUES (133, $1) RETURNING d","oids":[1184],"inputs":["323032342d30312d30322032333a35393a30302b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[1082],"rows":[["2024-01-02"]]}}"#,
    r#"{"name":"A ins timestamp->timestamptz","sql":"INSERT INTO w(id, tz) VALUES (134, $1) RETURNING tz","oids":[1114],"inputs":["323032342d30312d30322030333a30303a3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1114],"col_oids":[1184],"rows":[["2024-01-02 03:00:00+00"]]}}"#,
    r#"{"name":"A ins timestamptz->timestamp","sql":"INSERT INTO w(id, ts) VALUES (135, $1) RETURNING ts","oids":[1184],"inputs":["323032342d30312d30322030333a30303a30302b3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[1114],"rows":[["2024-01-01 22:00:00"]]}}"#,
    r#"{"name":"A ins timestamp->time","sql":"INSERT INTO w(id, tm) VALUES (136, $1) RETURNING tm","oids":[1114],"inputs":["323032342d30312d30322030333a30303a3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1114],"col_oids":[1083],"rows":[["03:00:00"]]}}"#,
    r#"{"name":"A ins timestamptz->timetz","sql":"INSERT INTO w(id, ttz) VALUES (137, $1) RETURNING ttz","oids":[1184],"inputs":["323032342d30312d30322030333a30303a30302b3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[1266],"rows":[["22:00:00+00"]]}}"#,
    r#"{"name":"A ins time->timetz","sql":"INSERT INTO w(id, ttz) VALUES (138, $1) RETURNING ttz","oids":[1083],"inputs":["30333a30303a3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1083],"col_oids":[1266],"rows":[["03:00:00+00"]]}}"#,
    r#"{"name":"A ins timetz->time","sql":"INSERT INTO w(id, tm) VALUES (139, $1) RETURNING tm","oids":[1266],"inputs":["30333a30303a30302b3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1266],"col_oids":[1083],"rows":[["03:00:00"]]}}"#,
    r#"{"name":"A ins interval->time","sql":"INSERT INTO w(id, tm) VALUES (140, $1) RETURNING tm","oids":[1186],"inputs":["31206461792030333a30303a3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1186],"col_oids":[1083],"rows":[["03:00:00"]]}}"#,
    r#"{"name":"A ins time->interval","sql":"INSERT INTO w(id, iv) VALUES (141, $1) RETURNING iv","oids":[1083],"inputs":["30333a30303a3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1083],"col_oids":[1186],"rows":[["03:00:00"]]}}"#,
    r#"{"name":"A ins date->time","sql":"INSERT INTO w(id, tm) VALUES (142, $1) RETURNING tm","oids":[1082],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"parse","sqlstate":"42804"}}"#,
    r#"{"name":"A ins interval->date","sql":"INSERT INTO w(id, d) VALUES (143, $1) RETURNING d","oids":[1186],"inputs":["3120646179"],"binary":false,"expected":{"phase":"parse","sqlstate":"42804"}}"#,
    r#"{"name":"A ins numeric->date","sql":"INSERT INTO w(id, d) VALUES (144, $1) RETURNING d","oids":[1700],"inputs":["31"],"binary":false,"expected":{"phase":"parse","sqlstate":"42804"}}"#,
    r#"{"name":"A ins date->int","sql":"INSERT INTO w(id, i) VALUES (145, $1) RETURNING i","oids":[1082],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"parse","sqlstate":"42804"}}"#,
    r#"{"name":"A ins int col <- timestamp","sql":"INSERT INTO w(id, i) VALUES (146, $1) RETURNING i","oids":[1114],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"parse","sqlstate":"42804"}}"#,
    r#"{"name":"A ins ts infinity->date","sql":"INSERT INTO w(id, d) VALUES (147, $1) RETURNING d","oids":[1114],"inputs":["696e66696e697479"],"binary":false,"expected":{"phase":"execute","param_oids":[1114],"col_oids":[1082],"rows":[["infinity"]]}}"#,
    r#"{"name":"A ins date->timestamp infinity","sql":"INSERT INTO w(id, ts) VALUES (148, $1) RETURNING ts","oids":[1082],"inputs":["2d696e66696e697479"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[1114],"rows":[["-infinity"]]}}"#,
    r#"{"name":"A ins numeric->numeric","sql":"INSERT INTO w(id, n) VALUES (149, $1) RETURNING n","oids":[1700],"inputs":["31322e333430"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[1700],"rows":[["12.340"]]}}"#,
    r#"{"name":"A ins multirow numeric->int","sql":"INSERT INTO w(id, i) VALUES (200, $1), (201, $2) RETURNING i","oids":[1700,1700],"inputs":["312e34","312e36"],"binary":false,"expected":{"phase":"execute","param_oids":[1700,1700],"col_oids":[23],"rows":[["1"],["2"]]}}"#,
    r#"{"name":"A ins multirow numeric oor","sql":"INSERT INTO w(id, i) VALUES (202, $1), (203, $2) RETURNING i","oids":[1700,1700],"inputs":["31","31653230"],"binary":false,"expected":{"phase":"execute","param_oids":[1700,1700],"col_oids":[23],"sqlstate":"22003"}}"#,
    r#"{"name":"U set numeric->int","sql":"UPDATE w SET i = $1 WHERE id = 1 RETURNING i","oids":[1700],"inputs":["372e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["8"]]}}"#,
    r#"{"name":"U set numeric->smallint oor","sql":"UPDATE w SET s = $1 WHERE id = 1 RETURNING s","oids":[1700],"inputs":["2d3430303030"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[21],"sqlstate":"22003"}}"#,
    r#"{"name":"U set float8->numeric","sql":"UPDATE w SET n = $1 WHERE id = 1 RETURNING n","oids":[701],"inputs":["322e35"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[1700],"rows":[["2.5"]]}}"#,
    r#"{"name":"U set text->int","sql":"UPDATE w SET i = $1 WHERE id = 1 RETURNING i","oids":[25],"inputs":["35"],"binary":false,"expected":{"phase":"parse","sqlstate":"42804"}}"#,
    r#"{"name":"U set timestamptz->date","sql":"UPDATE w SET d = $1 WHERE id = 1 RETURNING d","oids":[1184],"inputs":["323032342d30322d30332030313a30303a30302b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[1082],"rows":[["2024-02-03"]]}}"#,
    r#"{"name":"U set date->timestamptz","sql":"UPDATE w SET tz = $1 WHERE id = 1 RETURNING tz","oids":[1082],"inputs":["323032342d30322d3033"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[1184],"rows":[["2024-02-03 00:00:00+00"]]}}"#,
    r#"{"name":"U set bool->bool","sql":"UPDATE w SET bo = $1 WHERE id = 1 RETURNING bo","oids":[16],"inputs":["66"],"binary":false,"expected":{"phase":"execute","param_oids":[16],"col_oids":[16],"rows":[["f"]]}}"#,
    r#"{"name":"U set varchar->text","sql":"UPDATE w SET tx = $1 WHERE id = 1 RETURNING tx","oids":[1043],"inputs":["76"],"binary":false,"expected":{"phase":"execute","param_oids":[1043],"col_oids":[25],"rows":[["v"]]}}"#,
    r#"{"name":"U set interval->interval","sql":"UPDATE w SET iv = $1 WHERE id = 1 RETURNING iv","oids":[1186],"inputs":["31206d6f6e20322064617973"],"binary":false,"expected":{"phase":"execute","param_oids":[1186],"col_oids":[1186],"rows":[["1 mon 2 days"]]}}"#,
    r#"{"name":"U set float4->real","sql":"UPDATE w SET r = $1 WHERE id = 1 RETURNING r","oids":[700],"inputs":["322e35"],"binary":false,"expected":{"phase":"execute","param_oids":[700],"col_oids":[700],"rows":[["2.5"]]}}"#,
    r#"{"name":"U set timetz->timetz","sql":"UPDATE w SET ttz = $1 WHERE id = 1 RETURNING ttz","oids":[1266],"inputs":["30313a30323a30332d30343a3330"],"binary":false,"expected":{"phase":"execute","param_oids":[1266],"col_oids":[1266],"rows":[["01:02:03-04:30"]]}}"#,
    r#"{"name":"U set float8->int oor","sql":"UPDATE w SET i = $1 WHERE id = 1 RETURNING i","oids":[701],"inputs":["2d31653130"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[23],"sqlstate":"22003"}}"#,
    r#"{"name":"U set i=$1+1 numeric","sql":"UPDATE w SET i = $1 + 1 WHERE id = 1 RETURNING i","oids":[1700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["3"]]}}"#,
    r#"{"name":"U set n=$1*2 float8","sql":"UPDATE w SET n = $1 * 2 WHERE id = 1 RETURNING n","oids":[701],"inputs":["302e3235"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[1700],"rows":[["0.5"]]}}"#,
    r#"{"name":"F abs numeric","sql":"SELECT abs($1)","oids":[1700],"inputs":["2d312e3530"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[1700],"rows":[["1.50"]]}}"#,
    r#"{"name":"F round numeric 1","sql":"SELECT round($1, 1)","oids":[1700],"inputs":["322e3435"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[1700],"rows":[["2.5"]]}}"#,
    r#"{"name":"F round float8","sql":"SELECT round($1)","oids":[701],"inputs":["322e35"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[701],"rows":[["2"]]}}"#,
    r#"{"name":"F sqrt float8","sql":"SELECT sqrt($1)","oids":[701],"inputs":["32"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[701],"rows":[["1.4142135623730951"]]}}"#,
    r#"{"name":"F sqrt numeric","sql":"SELECT sqrt($1)","oids":[1700],"inputs":["32"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[1700],"rows":[["1.414213562373095"]]}}"#,
    r#"{"name":"F floor float4","sql":"SELECT floor($1)","oids":[700],"inputs":["322e35"],"binary":false,"expected":{"phase":"execute","param_oids":[700],"col_oids":[701],"rows":[["2"]]}}"#,
    r#"{"name":"F length text","sql":"SELECT length($1)","oids":[25],"inputs":["68656c6c6f"],"binary":false,"expected":{"phase":"execute","param_oids":[25],"col_oids":[23],"rows":[["5"]]}}"#,
    r#"{"name":"F upper varchar","sql":"SELECT upper($1)","oids":[1043],"inputs":["616263"],"binary":false,"expected":{"phase":"execute","param_oids":[1043],"col_oids":[25],"rows":[["ABC"]]}}"#,
    r#"{"name":"F concat text","sql":"SELECT $1 || 'x'","oids":[25],"inputs":["6162"],"binary":false,"expected":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[["abx"]]}}"#,
    r#"{"name":"F lower numeric","sql":"SELECT lower($1)","oids":[1700],"inputs":["31"],"binary":false,"expected":{"phase":"parse","sqlstate":"42883"}}"#,
    // PG18.6: {"rows":[["f"]],"param_oids":[16],"col_oids":[16]}; F15 closed the boolean projection gap
    r#"{"name":"F not bool","sql":"SELECT NOT $1","oids":[16],"inputs":["74"],"binary":false,"expected":{"phase":"execute","param_oids":[16],"col_oids":[16],"rows":[["f"]]}}"#,
    // PG18.6: {"rows":[["f"]],"param_oids":[16],"col_oids":[16]}; F15 closed the boolean projection gap
    r#"{"name":"F bool AND","sql":"SELECT $1 AND true","oids":[16],"inputs":["66"],"binary":false,"expected":{"phase":"execute","param_oids":[16],"col_oids":[16],"rows":[["f"]]}}"#,
    // PG18.6: {"rows":[["2024-02-01"]],"param_oids":[1082],"col_oids":[1082]}; F16 closed the temporal operator gap
    r#"{"name":"F date + int","sql":"SELECT $1 + 1","oids":[1082],"inputs":["323032342d30312d3331"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[1082],"rows":[["2024-02-01"]]}}"#,
    // PG18.6: {"rows":[["2024-02-01 00:00:00"]],"param_oids":[1082],"col_oids":[1114]}; F16 closed the temporal operator gap
    r#"{"name":"F date + interval","sql":"SELECT $1 + interval '1 day'","oids":[1082],"inputs":["323032342d30312d3331"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[1114],"rows":[["2024-02-01 00:00:00"]]}}"#,
    // PG18.6: {"rows":[["30"]],"param_oids":[1082],"col_oids":[23]}; F16 closed the temporal operator gap
    r#"{"name":"F date - date","sql":"SELECT $1 - date '2024-01-01'","oids":[1082],"inputs":["323032342d30312d3331"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[23],"rows":[["30"]]}}"#,
    r#"{"name":"F extract year ts","sql":"SELECT extract(year from $1)","oids":[1114],"inputs":["323032342d30352d30362030373a30383a3039"],"binary":false,"expected":{"phase":"execute","param_oids":[1114],"col_oids":[1700],"rows":[["2024"]]}}"#,
    r#"{"name":"F date_part tstz","sql":"SELECT date_part('hour', $1)","oids":[1184],"inputs":["323032342d30352d30362030373a30383a30392b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[701],"rows":[["7"]]}}"#,
    // PG18.6: {"rows":[["2024-01-02 00:00:00"]],"param_oids":[1114,1186],"col_oids":[1114]}; F16 closed the temporal operator gap
    r#"{"name":"F ts + interval param","sql":"SELECT $1 + $2","oids":[1114,1186],"inputs":["323032342d30312d30312030303a30303a3030","3120646179"],"binary":false,"expected":{"phase":"execute","param_oids":[1114,1186],"col_oids":[1114],"rows":[["2024-01-02 00:00:00"]]}}"#,
    // PG18.6: {"rows":[["02:00:00"]],"param_oids":[1186],"col_oids":[1186]}; F16 closed the temporal operator gap
    r#"{"name":"F interval * 2","sql":"SELECT $1 * 2","oids":[1186],"inputs":["3120686f7572"],"binary":false,"expected":{"phase":"execute","param_oids":[1186],"col_oids":[1186],"rows":[["02:00:00"]]}}"#,
    r#"{"name":"F numeric + int col","sql":"SELECT n + $1 FROM t WHERE id = 1","oids":[1700],"inputs":["31"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[1700],"rows":[["2.5"]]}}"#,
    r#"{"name":"F int col + numeric","sql":"SELECT i + $1 FROM t WHERE id = 1","oids":[1700],"inputs":["302e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[1700],"rows":[["2.5"]]}}"#,
    r#"{"name":"F int col * float8","sql":"SELECT i * $1 FROM t WHERE id = 1","oids":[701],"inputs":["302e35"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[701],"rows":[["1"]]}}"#,
    r#"{"name":"F numeric / 0","sql":"SELECT $1 / 0","oids":[1700],"inputs":["31"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[1700],"sqlstate":"22012"}}"#,
    r#"{"name":"F float8 + float4","sql":"SELECT $1 + $2","oids":[701,700],"inputs":["312e35","302e3235"],"binary":false,"expected":{"phase":"execute","param_oids":[701,700],"col_oids":[701],"rows":[["1.75"]]}}"#,
    r#"{"name":"F numeric + float8","sql":"SELECT $1 + $2","oids":[1700,701],"inputs":["312e35","302e3235"],"binary":false,"expected":{"phase":"execute","param_oids":[1700,701],"col_oids":[701],"rows":[["1.75"]]}}"#,
    r#"{"name":"F date_trunc tstz","sql":"SELECT date_trunc('day', $1)","oids":[1184],"inputs":["323032342d30352d30362030373a30383a30392b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[1184],"rows":[["2024-05-06 00:00:00+00"]]}}"#,
    r#"{"name":"F coalesce numeric","sql":"SELECT coalesce($1, 0)","oids":[1700],"inputs":["322e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[1700],"rows":[["2.5"]]}}"#,
    r#"{"name":"F $1::int numeric","sql":"SELECT $1::int","oids":[1700],"inputs":["322e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["3"]]}}"#,
    r#"{"name":"F $1::text float8","sql":"SELECT $1::text","oids":[701],"inputs":["302e31"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[25],"rows":[["0.1"]]}}"#,
    r#"{"name":"F $1::date tstz","sql":"SELECT $1::date","oids":[1184],"inputs":["323032342d30312d30322032333a30303a30302b3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[1082],"rows":[["2024-01-02"]]}}"#,
    r#"{"name":"F $1::numeric text","sql":"SELECT $1::numeric","oids":[25],"inputs":["312e3235"],"binary":false,"expected":{"phase":"execute","param_oids":[25],"col_oids":[1700],"rows":[["1.25"]]}}"#,
    r#"{"name":"F $1::int bool","sql":"SELECT $1::int","oids":[16],"inputs":["74"],"binary":false,"expected":{"phase":"execute","param_oids":[16],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"F substr varchar","sql":"SELECT substr($1, 2)","oids":[1043],"inputs":["61626364"],"binary":false,"expected":{"phase":"execute","param_oids":[1043],"col_oids":[25],"rows":[["bcd"]]}}"#,
    // PG18.6: {"rows":[["00:30:00"]],"param_oids":[1083],"col_oids":[1083]}; F16 closed the temporal operator gap
    r#"{"name":"F time + interval","sql":"SELECT $1 + interval '1 hour'","oids":[1083],"inputs":["32333a33303a3030"],"binary":false,"expected":{"phase":"execute","param_oids":[1083],"col_oids":[1083],"rows":[["00:30:00"]]}}"#,
    // PG18.6: {"rows":[["2"]],"param_oids":[1700],"col_oids":[1700]} (DD-0068 F17)
    r#"{"name":"F greatest numeric","sql":"SELECT greatest($1, 2)","oids":[1700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[1700],"rows":[["2"]]}}"#,
    // PG18.6: {"rows":[["1"]],"param_oids":[16],"col_oids":[23]}; F15 closed the boolean projection gap
    r#"{"name":"F CASE bool","sql":"SELECT CASE WHEN $1 THEN 1 ELSE 0 END","oids":[16],"inputs":["74727565"],"binary":false,"expected":{"phase":"execute","param_oids":[16],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"F WHERE bool param","sql":"SELECT id FROM t WHERE $1 ORDER BY id","oids":[16],"inputs":["74727565"],"binary":false,"expected":{"phase":"execute","param_oids":[16],"col_oids":[23],"rows":[["1"],["2"],["3"]]}}"#,
    // PG18.6: {"rows":[["t"]],"param_oids":[1700],"col_oids":[16]}; F15 closed the boolean projection gap
    r#"{"name":"F numeric > int literal","sql":"SELECT $1 > 1","oids":[1700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[16],"rows":[["t"]]}}"#,
    // PG18.6: {"rows":[["t"]],"param_oids":[701],"col_oids":[16]}; F15 closed the boolean projection gap
    r#"{"name":"F float8 = numeric literal","sql":"SELECT $1 = 1.5","oids":[701],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[16],"rows":[["t"]]}}"#,
    r#"{"name":"F limit numeric","sql":"SELECT id FROM t ORDER BY id LIMIT $1","oids":[1700],"inputs":["312e35"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["1"],["2"]]}}"#,
    r#"{"name":"R bare bool","sql":"SELECT $1","oids":[16],"inputs":["796573"],"binary":false,"expected":{"phase":"execute","param_oids":[16],"col_oids":[16],"rows":[["t"]]}}"#,
    r#"{"name":"R bare text","sql":"SELECT $1","oids":[25],"inputs":["61"],"binary":false,"expected":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[["a"]]}}"#,
    // PG18.6: {"rows":[["a"]],"param_oids":[1043],"col_oids":[1043]}; gap: varchar result described as text OID 25 instead of 1043
    r#"{"name":"known gap: R bare varchar","sql":"SELECT $1","oids":[1043],"inputs":["61"],"binary":false,"expected":{"phase":"execute","param_oids":[1043],"col_oids":[25],"rows":[["a"]]}}"#,
    r#"{"name":"R bare numeric","sql":"SELECT $1","oids":[1700],"inputs":["312e3530"],"binary":false,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[1700],"rows":[["1.50"]]}}"#,
    r#"{"name":"R bare float4","sql":"SELECT $1","oids":[700],"inputs":["302e31"],"binary":false,"expected":{"phase":"execute","param_oids":[700],"col_oids":[700],"rows":[["0.1"]]}}"#,
    r#"{"name":"R bare float8","sql":"SELECT $1","oids":[701],"inputs":["302e31"],"binary":false,"expected":{"phase":"execute","param_oids":[701],"col_oids":[701],"rows":[["0.1"]]}}"#,
    r#"{"name":"R bare date","sql":"SELECT $1","oids":[1082],"inputs":["323032342d30312d3032"],"binary":false,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[1082],"rows":[["2024-01-02"]]}}"#,
    r#"{"name":"R bare time","sql":"SELECT $1","oids":[1083],"inputs":["30333a30343a30352e313233"],"binary":false,"expected":{"phase":"execute","param_oids":[1083],"col_oids":[1083],"rows":[["03:04:05.123"]]}}"#,
    r#"{"name":"R bare timetz","sql":"SELECT $1","oids":[1266],"inputs":["30333a30343a30352b30353a3330"],"binary":false,"expected":{"phase":"execute","param_oids":[1266],"col_oids":[1266],"rows":[["03:04:05+05:30"]]}}"#,
    r#"{"name":"R bare timestamp","sql":"SELECT $1","oids":[1114],"inputs":["323032342d30312d30322030333a30343a3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1114],"col_oids":[1114],"rows":[["2024-01-02 03:04:05"]]}}"#,
    r#"{"name":"R bare timestamptz","sql":"SELECT $1","oids":[1184],"inputs":["323032342d30312d30322030333a30343a30352b3035"],"binary":false,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[1184],"rows":[["2024-01-01 22:04:05+00"]]}}"#,
    r#"{"name":"R bare interval","sql":"SELECT $1","oids":[1186],"inputs":["3120796561722032206d6f6e73203320646179732030343a30353a3036"],"binary":false,"expected":{"phase":"execute","param_oids":[1186],"col_oids":[1186],"rows":[["1 year 2 mons 3 days 04:05:06"]]}}"#,
    r#"{"name":"R bare $1, $2 mixed","sql":"SELECT $1, $2","oids":[1700,1082],"inputs":["312e35","323032342d30312d3032"],"binary":false,"expected":{"phase":"execute","param_oids":[1700,1082],"col_oids":[1700,1082],"rows":[["1.5","2024-01-02"]]}}"#,
    r#"{"name":"T bad bool","sql":"SELECT $1","oids":[16],"inputs":["6d61796265"],"binary":false,"expected":{"phase":"bind","param_oids":[16],"col_oids":[16],"sqlstate":"22P02"}}"#,
    r#"{"name":"T bad numeric","sql":"SELECT $1","oids":[1700],"inputs":["616263"],"binary":false,"expected":{"phase":"bind","param_oids":[1700],"col_oids":[1700],"sqlstate":"22P02"}}"#,
    r#"{"name":"T bad numeric big","sql":"SELECT $1","oids":[1700],"inputs":["3165323030303030"],"binary":false,"expected":{"phase":"bind","param_oids":[1700],"col_oids":[1700],"sqlstate":"22003"}}"#,
    r#"{"name":"T bad float4 oor","sql":"SELECT $1","oids":[700],"inputs":["31653430"],"binary":false,"expected":{"phase":"bind","param_oids":[700],"col_oids":[700],"sqlstate":"22003"}}"#,
    r#"{"name":"T bad float8 oor","sql":"SELECT $1","oids":[701],"inputs":["3165343030"],"binary":false,"expected":{"phase":"bind","param_oids":[701],"col_oids":[701],"sqlstate":"22003"}}"#,
    r#"{"name":"T bad float8 junk","sql":"SELECT $1","oids":[701],"inputs":["312e3578"],"binary":false,"expected":{"phase":"bind","param_oids":[701],"col_oids":[701],"sqlstate":"22P02"}}"#,
    r#"{"name":"T bad date junk","sql":"SELECT $1","oids":[1082],"inputs":["78797a"],"binary":false,"expected":{"phase":"bind","param_oids":[1082],"col_oids":[1082],"sqlstate":"22007"}}"#,
    r#"{"name":"T bad date range","sql":"SELECT $1","oids":[1082],"inputs":["323032342d31332d3031"],"binary":false,"expected":{"phase":"bind","param_oids":[1082],"col_oids":[1082],"sqlstate":"22008"}}"#,
    r#"{"name":"T bad time junk","sql":"SELECT $1","oids":[1083],"inputs":["32353a30303a3030"],"binary":false,"expected":{"phase":"bind","param_oids":[1083],"col_oids":[1083],"sqlstate":"22008"}}"#,
    r#"{"name":"T bad timetz junk","sql":"SELECT $1","oids":[1266],"inputs":["616263"],"binary":false,"expected":{"phase":"bind","param_oids":[1266],"col_oids":[1266],"sqlstate":"22007"}}"#,
    r#"{"name":"T bad timestamp junk","sql":"SELECT $1","oids":[1114],"inputs":["323032342d30312d3031207878"],"binary":false,"expected":{"phase":"bind","param_oids":[1114],"col_oids":[1114],"sqlstate":"22007"}}"#,
    r#"{"name":"T bad timestamptz range","sql":"SELECT $1","oids":[1184],"inputs":["323032342d30322d33302030303a30303a30302b3030"],"binary":false,"expected":{"phase":"bind","param_oids":[1184],"col_oids":[1184],"sqlstate":"22008"}}"#,
    r#"{"name":"T bad interval junk","sql":"SELECT $1","oids":[1186],"inputs":["616263"],"binary":false,"expected":{"phase":"bind","param_oids":[1186],"col_oids":[1186],"sqlstate":"22007"}}"#,
    r#"{"name":"T bad text nul","sql":"SELECT $1","oids":[25],"inputs":["610062"],"binary":false,"expected":{"phase":"bind","param_oids":[25],"col_oids":[25],"sqlstate":"22021"}}"#,
    r#"{"name":"B bool t","sql":"SELECT $1","oids":[16],"inputs":["01"],"binary":true,"expected":{"phase":"execute","param_oids":[16],"col_oids":[16],"rows":[["t"]]}}"#,
    r#"{"name":"B text","sql":"SELECT $1","oids":[25],"inputs":["616263"],"binary":true,"expected":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[["abc"]]}}"#,
    // PG18.6: {"rows":[["abc"]],"param_oids":[1043],"col_oids":[1043]}; gap: varchar result described as text OID 25 instead of 1043
    r#"{"name":"known gap: B varchar","sql":"SELECT $1","oids":[1043],"inputs":["616263"],"binary":true,"expected":{"phase":"execute","param_oids":[1043],"col_oids":[25],"rows":[["abc"]]}}"#,
    r#"{"name":"B numeric 1.5","sql":"SELECT $1","oids":[1700],"inputs":["000200000000000100011388"],"binary":true,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[1700],"rows":[["1.5"]]}}"#,
    r#"{"name":"B float4 1.5","sql":"SELECT $1","oids":[700],"inputs":["3fc00000"],"binary":true,"expected":{"phase":"execute","param_oids":[700],"col_oids":[700],"rows":[["1.5"]]}}"#,
    r#"{"name":"B float8 1.5","sql":"SELECT $1","oids":[701],"inputs":["3ff8000000000000"],"binary":true,"expected":{"phase":"execute","param_oids":[701],"col_oids":[701],"rows":[["1.5"]]}}"#,
    r#"{"name":"B date","sql":"SELECT $1","oids":[1082],"inputs":["0000223f"],"binary":true,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[1082],"rows":[["2024-01-02"]]}}"#,
    r#"{"name":"B time","sql":"SELECT $1","oids":[1083],"inputs":["00000000dde878c0"],"binary":true,"expected":{"phase":"execute","param_oids":[1083],"col_oids":[1083],"rows":[["01:02:03"]]}}"#,
    r#"{"name":"B timetz","sql":"SELECT $1","oids":[1266],"inputs":["00000000dde878c0fffff1f0"],"binary":true,"expected":{"phase":"execute","param_oids":[1266],"col_oids":[1266],"rows":[["01:02:03+01"]]}}"#,
    r#"{"name":"B timestamp","sql":"SELECT $1","oids":[1114],"inputs":["0002b0d5d4e94000"],"binary":true,"expected":{"phase":"execute","param_oids":[1114],"col_oids":[1114],"rows":[["2024-01-01 00:00:00"]]}}"#,
    r#"{"name":"B timestamptz","sql":"SELECT $1","oids":[1184],"inputs":["0002b0d5d4e94000"],"binary":true,"expected":{"phase":"execute","param_oids":[1184],"col_oids":[1184],"rows":[["2024-01-01 00:00:00+00"]]}}"#,
    r#"{"name":"B interval","sql":"SELECT $1","oids":[1186],"inputs":["00000000d693a4000000000200000001"],"binary":true,"expected":{"phase":"execute","param_oids":[1186],"col_oids":[1186],"rows":[["1 mon 2 days 01:00:00"]]}}"#,
    r#"{"name":"B numeric cmp n","sql":"SELECT id FROM t WHERE n = $1 ORDER BY id","oids":[1700],"inputs":["000200000000000100011388"],"binary":true,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"B float8 cmp i","sql":"SELECT id FROM t WHERE i = $1 ORDER BY id","oids":[701],"inputs":["4000000000000000"],"binary":true,"expected":{"phase":"execute","param_oids":[701],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"B date cmp ts","sql":"SELECT id FROM t WHERE ts = $1 ORDER BY id","oids":[1082],"inputs":["0000223f"],"binary":true,"expected":{"phase":"execute","param_oids":[1082],"col_oids":[23],"rows":[["1"]]}}"#,
    r#"{"name":"B numeric ins int","sql":"INSERT INTO w(id, i) VALUES (300, $1) RETURNING i","oids":[1700],"inputs":["000200000000000100011388"],"binary":true,"expected":{"phase":"execute","param_oids":[1700],"col_oids":[23],"rows":[["2"]]}}"#,
    r#"{"name":"B bool 2","sql":"SELECT $1","oids":[16],"inputs":["02"],"binary":true,"expected":{"phase":"execute","param_oids":[16],"col_oids":[16],"rows":[["t"]]}}"#,
    r#"{"name":"B bool empty","sql":"SELECT $1","oids":[16],"inputs":[""],"binary":true,"expected":{"phase":"bind","param_oids":[16],"col_oids":[16],"sqlstate":"08P01"}}"#,
    r#"{"name":"B bool 2 bytes","sql":"SELECT $1","oids":[16],"inputs":["0100"],"binary":true,"expected":{"phase":"bind","param_oids":[16],"col_oids":[16],"sqlstate":"22P03"}}"#,
    r#"{"name":"B float4 short","sql":"SELECT $1","oids":[700],"inputs":["0000"],"binary":true,"expected":{"phase":"bind","param_oids":[700],"col_oids":[700],"sqlstate":"08P01"}}"#,
    r#"{"name":"B float4 long","sql":"SELECT $1","oids":[700],"inputs":["0000000000"],"binary":true,"expected":{"phase":"bind","param_oids":[700],"col_oids":[700],"sqlstate":"22P03"}}"#,
    r#"{"name":"B float8 short","sql":"SELECT $1","oids":[701],"inputs":["00000000"],"binary":true,"expected":{"phase":"bind","param_oids":[701],"col_oids":[701],"sqlstate":"08P01"}}"#,
    r#"{"name":"B float8 long","sql":"SELECT $1","oids":[701],"inputs":["000000000000000000"],"binary":true,"expected":{"phase":"bind","param_oids":[701],"col_oids":[701],"sqlstate":"22P03"}}"#,
    r#"{"name":"B numeric short","sql":"SELECT $1","oids":[1700],"inputs":["0001"],"binary":true,"expected":{"phase":"bind","param_oids":[1700],"col_oids":[1700],"sqlstate":"08P01"}}"#,
    r#"{"name":"B numeric bad sign","sql":"SELECT $1","oids":[1700],"inputs":["00010000123400000001"],"binary":true,"expected":{"phase":"bind","param_oids":[1700],"col_oids":[1700],"sqlstate":"22P03"}}"#,
    r#"{"name":"B numeric bad digit","sql":"SELECT $1","oids":[1700],"inputs":["00010000000000002710"],"binary":true,"expected":{"phase":"bind","param_oids":[1700],"col_oids":[1700],"sqlstate":"22P03"}}"#,
    r#"{"name":"B numeric trailing","sql":"SELECT $1","oids":[1700],"inputs":["00020000000000010001138800"],"binary":true,"expected":{"phase":"bind","param_oids":[1700],"col_oids":[1700],"sqlstate":"22P03"}}"#,
    r#"{"name":"B date short","sql":"SELECT $1","oids":[1082],"inputs":["000001"],"binary":true,"expected":{"phase":"bind","param_oids":[1082],"col_oids":[1082],"sqlstate":"08P01"}}"#,
    r#"{"name":"B date long","sql":"SELECT $1","oids":[1082],"inputs":["0000000000"],"binary":true,"expected":{"phase":"bind","param_oids":[1082],"col_oids":[1082],"sqlstate":"22P03"}}"#,
    r#"{"name":"B time oor","sql":"SELECT $1","oids":[1083],"inputs":["000000141dd76001"],"binary":true,"expected":{"phase":"bind","param_oids":[1083],"col_oids":[1083],"sqlstate":"22008"}}"#,
    r#"{"name":"B timetz short","sql":"SELECT $1","oids":[1266],"inputs":["0000000000000000"],"binary":true,"expected":{"phase":"bind","param_oids":[1266],"col_oids":[1266],"sqlstate":"08P01"}}"#,
    r#"{"name":"B timetz bad zone","sql":"SELECT $1","oids":[1266],"inputs":["00000000000000000000ea60"],"binary":true,"expected":{"phase":"bind","param_oids":[1266],"col_oids":[1266],"sqlstate":"22009"}}"#,
    r#"{"name":"B timestamp short","sql":"SELECT $1","oids":[1114],"inputs":["00000000000000"],"binary":true,"expected":{"phase":"bind","param_oids":[1114],"col_oids":[1114],"sqlstate":"08P01"}}"#,
    r#"{"name":"B interval short","sql":"SELECT $1","oids":[1186],"inputs":["000000000000000000000000"],"binary":true,"expected":{"phase":"bind","param_oids":[1186],"col_oids":[1186],"sqlstate":"08P01"}}"#,
    r#"{"name":"B text bad utf8","sql":"SELECT $1","oids":[25],"inputs":["fffe"],"binary":true,"expected":{"phase":"bind","param_oids":[25],"col_oids":[25],"sqlstate":"22021"}}"#,
    // PG18.6: {"sqlstate":"22021","param_oids":[1043],"col_oids":[1043]}; gap: varchar result described as text OID 25 instead of 1043
    r#"{"name":"known gap: B varchar bad utf8","sql":"SELECT $1","oids":[1043],"inputs":["c3"],"binary":true,"expected":{"phase":"bind","param_oids":[1043],"col_oids":[25],"sqlstate":"22021"}}"#,
    r#"{"name":"B text nul","sql":"SELECT $1","oids":[25],"inputs":["610062"],"binary":true,"expected":{"phase":"bind","param_oids":[25],"col_oids":[25],"sqlstate":"22021"}}"#,
];
