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
            "float-errors-network-{}-{}-{}",
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

#[tokio::test]
async fn postgres_float_error_messages() {
    let fixture = Fixture::new().await;
    let mut stream = fixture.connect().await;
    for sql in [
        "CREATE TABLE float_input (id bigint primary key)",
        "INSERT INTO float_input VALUES (1)",
    ] {
        let mut body = vec![];
        cstr(sql, &mut body);
        send(&mut stream, b'Q', &body).await;
        loop {
            let (tag, body) = read(&mut stream).await;
            assert_ne!(tag, b'E', "{sql}: {:?}", body);
            if tag == b'Z' {
                break;
            }
        }
    }

    for (expr, code, message) in [
        (
            "sqrt(-1::numeric)",
            "2201F",
            "cannot take square root of a negative number",
        ),
        (
            "sqrt(-1::float8)",
            "2201F",
            "cannot take square root of a negative number",
        ),
        ("ln(0::float8)", "2201E", "cannot take logarithm of zero"),
        (
            "power(0::float8,-1::float8)",
            "2201F",
            "zero raised to a negative power is undefined",
        ),
        ("acos(2::float8)", "22003", "input is out of range"),
        ("exp(710::float8)", "22003", "value out of range: overflow"),
        ("gamma(0::float8)", "22003", "value out of range: overflow"),
        (
            "lgamma(1e308::float8)",
            "22003",
            "value out of range: overflow",
        ),
        ("asind(2::float8)", "22003", "input is out of range"),
    ] {
        let mut body = vec![];
        cstr(&format!("SELECT {expr} FROM float_input"), &mut body);
        send(&mut stream, b'Q', &body).await;
        let mut errors = 0;
        loop {
            let (tag, body) = read(&mut stream).await;
            if tag == b'E' {
                let fields: BTreeMap<u8, &str> = body
                    .split(|b| *b == 0)
                    .filter(|field| !field.is_empty())
                    .map(|field| (field[0], std::str::from_utf8(&field[1..]).unwrap()))
                    .collect();
                assert_eq!(fields.get(&b'C'), Some(&code), "{expr}");
                assert_eq!(fields.get(&b'M'), Some(&message), "{expr}");
                errors += 1;
            }
            if tag == b'Z' {
                break;
            }
        }
        assert_eq!(errors, 1, "{expr}");
    }
    let mut body = vec![];
    cstr("SELECT cosh(711::float8) FROM float_input", &mut body);
    send(&mut stream, b'Q', &body).await;
    let mut rows = 0;
    loop {
        let (tag, body) = read(&mut stream).await;
        assert_ne!(tag, b'E', "cosh(711): {:?}", body);
        if tag == b'D' {
            assert_eq!(&body[..2], &1u16.to_be_bytes());
            assert_eq!(&body[2..6], &8i32.to_be_bytes());
            assert_eq!(&body[6..], b"Infinity");
            rows += 1;
        }
        if tag == b'Z' {
            break;
        }
    }
    assert_eq!(rows, 1);
    drop(stream);
    fixture.stop().await;
}
