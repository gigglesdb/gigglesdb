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
            "prepared-validation-network-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&path).unwrap();
        #[cfg(feature = "engine-rocksdb")]
        let admin =
            Admin::create_engine(path.join("db"), Limits::default(), giggles_core::Engine::V2)
                .unwrap();
        #[cfg(not(feature = "engine-rocksdb"))]
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

async fn until(stream: &mut TcpStream, end: u8) -> Vec<(u8, Vec<u8>)> {
    let mut frames = vec![];
    loop {
        let frame = read(stream).await;
        let done = frame.0 == end;
        frames.push(frame);
        if done {
            return frames;
        }
    }
}
async fn simple(stream: &mut TcpStream, sql: &str) {
    let mut body = vec![];
    cstr(sql, &mut body);
    send(stream, b'Q', &body).await;
    let frames = until(stream, b'Z').await;
    assert!(!frames.iter().any(|f| f.0 == b'E'), "{sql}: {frames:?}");
}
async fn parse(stream: &mut TcpStream, name: &str, sql: &str) {
    let mut body = vec![];
    cstr(name, &mut body);
    cstr(sql, &mut body);
    body.extend(0u16.to_be_bytes());
    send(stream, b'P', &body).await;
    send(stream, b'H', &[]).await;
    assert_eq!(read(stream).await.0, b'1', "{sql}");
}
async fn bind(stream: &mut TcpStream, statement: &str) {
    let mut body = vec![0];
    cstr(statement, &mut body);
    body.extend([0; 6]);
    send(stream, b'B', &body).await;
    send(stream, b'H', &[]).await;
}
async fn execute(stream: &mut TcpStream, max: u32) {
    let mut body = vec![0];
    body.extend(max.to_be_bytes());
    send(stream, b'E', &body).await;
    send(stream, b'H', &[]).await;
}
async fn sync(stream: &mut TcpStream) {
    send(stream, b'S', &[]).await;
    assert_eq!(read(stream).await.0, b'Z');
}
// A freshly parsed unnamed statement runs the old full validation/binding on
// every iteration. Compare its complete portal description and result frames
// with repeated Binds of a single named statement.
async fn outcome(stream: &mut TcpStream, statement: &str) -> Vec<(u8, Vec<u8>)> {
    bind(stream, statement).await;
    let first = read(stream).await;
    if first.0 == b'E' {
        sync(stream).await;
        return vec![first];
    }
    assert_eq!(first.0, b'2');
    send(stream, b'D', &[b'P', 0]).await;
    execute(stream, 0).await;
    send(stream, b'S', &[]).await;
    let mut frames = until(stream, b'Z').await;
    frames.pop();
    frames
}
#[tokio::test]
async fn repeated_named_bind_matches_fresh_full_validation() {
    let f = Fixture::new().await;
    let mut stream = f.connect().await;
    simple(&mut stream, "CREATE TABLE t(id int PRIMARY KEY, n int)").await;
    simple(&mut stream, "INSERT INTO t VALUES (1,2),(2,1),(3,3)").await;
    for (i, sql) in [
        "SELECT id FROM t ORDER BY id",
        "SELECT id FROM t ORDER BY n",
        "SELECT 1+2 FROM t",
        "SELECT 1/0 FROM t",
        "SELECT id FROM t ORDER BY 1/0",
        "SELECT id FROM t WHERE 1/0=1",
        "SELECT '2001-02-03'::date::text",
        "SELECT '2001-02-03 04:05:06+00'::timestamptz::text",
        "SELECT '1 day'::interval::text",
        "SELECT 1.23456789::float8::text",
    ]
    .iter()
    .enumerate()
    {
        let name = format!("s{i}");
        parse(&mut stream, &name, sql).await;
        for setting in [
            "SET TimeZone='UTC'",
            "SET TimeZone='America/New_York'",
            "SET DateStyle='ISO, MDY'",
            "SET DateStyle='ISO, DMY'",
            "SET IntervalStyle='postgres'",
            "SET IntervalStyle='iso_8601'",
            "SET extra_float_digits=0",
            "SET extra_float_digits=3",
        ] {
            simple(&mut stream, setting).await;
            parse(&mut stream, "", sql).await;
            let reference = outcome(&mut stream, "").await;
            assert_eq!(
                outcome(&mut stream, &name).await,
                reference,
                "{sql}: {setting}"
            );
        }
    }
    drop(stream);
    f.stop().await;
}
#[tokio::test]
async fn named_cursor_suspends_and_bind_errors_precede_bind_complete() {
    let f = Fixture::new().await;
    let mut stream = f.connect().await;
    simple(&mut stream, "CREATE TABLE t(id int PRIMARY KEY)").await;
    simple(&mut stream, "INSERT INTO t VALUES (1),(2),(3),(4),(5)").await;
    parse(&mut stream, "scan", "SELECT id FROM t ORDER BY id").await;
    for _ in 0..2 {
        bind(&mut stream, "scan").await;
        assert_eq!(read(&mut stream).await.0, b'2');
        let mut rows = vec![];
        for (end, count) in [(b's', 2), (b's', 2), (b'C', 1)] {
            execute(&mut stream, 2).await;
            let frames = until(&mut stream, end).await;
            assert_eq!(frames.iter().filter(|f| f.0 == b'D').count(), count);
            rows.extend(frames.into_iter().filter(|f| f.0 == b'D'));
        }
        sync(&mut stream).await;
        parse(&mut stream, "", "SELECT id FROM t ORDER BY id").await;
        let reference = outcome(&mut stream, "").await;
        assert_eq!(
            rows,
            reference
                .into_iter()
                .filter(|f| f.0 == b'D')
                .collect::<Vec<_>>()
        );
    }
    parse(&mut stream, "bad", "SELECT 1/0").await;
    for _ in 0..2 {
        bind(&mut stream, "bad").await;
        let frame = read(&mut stream).await;
        assert_eq!(frame.0, b'E');
        assert!(frame.1.windows(6).any(|w| w == b"C22012"));
        sync(&mut stream).await;
    }
    simple(&mut stream, "BEGIN").await;
    bind(&mut stream, "bad").await;
    assert_eq!(read(&mut stream).await.0, b'E');
    sync(&mut stream).await;
    bind(&mut stream, "scan").await;
    let error = read(&mut stream).await;
    assert_eq!(error.0, b'E');
    assert!(error.1.windows(6).any(|w| w == b"C25P02"));
    sync(&mut stream).await;
    simple(&mut stream, "ROLLBACK").await;
    drop(stream);
    f.stop().await;
}
#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn named_validation_rebinds_after_ddl_and_rollback() {
    let f = Fixture::new().await;
    let mut stream = f.connect().await;
    simple(&mut stream, "CREATE SCHEMA earlier").await;
    simple(&mut stream, "CREATE TABLE t(id int PRIMARY KEY, n int)").await;
    let sql = "SELECT id FROM t ORDER BY n";
    parse(&mut stream, "named", sql).await;
    let initial = outcome(&mut stream, "named").await;
    assert_eq!(outcome(&mut stream, "named").await, initial);
    for change in [
        "ALTER TABLE t ADD COLUMN x int",
        "ALTER TABLE t DROP COLUMN x",
        "ALTER TABLE t ALTER COLUMN n TYPE text",
        "DROP TABLE t",
        "CREATE TABLE t(id int PRIMARY KEY,n text)",
        "SET search_path=earlier,public",
        "CREATE TABLE earlier.t(id int PRIMARY KEY,n int)",
        "SET search_path=public",
        "BEGIN",
        "ALTER TABLE t ADD COLUMN x int",
        "ROLLBACK",
    ] {
        simple(&mut stream, change).await;
        if change == "DROP TABLE t" {
            bind(&mut stream, "named").await;
            let error = read(&mut stream).await;
            assert_eq!(error.0, b'E');
            assert!(error.1.windows(6).any(|w| w == b"C42P01"));
            sync(&mut stream).await;
            continue;
        }
        parse(&mut stream, "", sql).await;
        let reference = outcome(&mut stream, "").await;
        assert_eq!(outcome(&mut stream, "named").await, reference, "{change}");
    }
    // Re-prepare the same name after closing it.
    send(&mut stream, b'C', b"Snamed\0").await;
    send(&mut stream, b'H', &[]).await;
    assert_eq!(read(&mut stream).await.0, b'3');
    parse(&mut stream, "named", sql).await;
    parse(&mut stream, "", sql).await;
    let reference = outcome(&mut stream, "").await;
    assert_eq!(outcome(&mut stream, "named").await, reference);
    drop(stream);
    f.stop().await;
}

#[tokio::test]
async fn named_parameter_filter_rebinds_values() {
    let f = Fixture::new().await;
    let mut stream = f.connect().await;
    simple(&mut stream, "CREATE TABLE t(id int PRIMARY KEY)").await;
    simple(&mut stream, "INSERT INTO t VALUES (1),(2)").await;
    parse(&mut stream, "filtered", "SELECT id FROM t WHERE id=$1").await;
    for value in ["1", "2", "1"] {
        parse(&mut stream, "", "SELECT id FROM t WHERE id=$1").await;
        let mut results = vec![];
        for name in ["", "filtered"] {
            let mut body = vec![0];
            cstr(name, &mut body);
            body.extend(0u16.to_be_bytes());
            body.extend(1u16.to_be_bytes());
            body.extend((value.len() as i32).to_be_bytes());
            body.extend(value.as_bytes());
            body.extend(0u16.to_be_bytes());
            send(&mut stream, b'B', &body).await;
            send(&mut stream, b'H', &[]).await;
            assert_eq!(read(&mut stream).await.0, b'2');
            send(&mut stream, b'D', &[b'P', 0]).await;
            execute(&mut stream, 0).await;
            send(&mut stream, b'S', &[]).await;
            results.push(until(&mut stream, b'Z').await);
        }
        assert_eq!(results[0], results[1]);
        assert_eq!(results[0].iter().filter(|f| f.0 == b'D').count(), 1);
    }
    drop(stream);
    f.stop().await;
}

#[tokio::test]
async fn sql_prepared_cursor_checks_session_authorization_and_reset_role() {
    let f = Fixture::new().await;
    let mut stream = f.connect().await;
    // The legacy role catalog only supports inheritable memberships.
    // SUPERUSER applies only when reader is the active role.
    let (reader, restricted) = if cfg!(feature = "engine-rocksdb") {
        (
            "CREATE ROLE reader",
            "CREATE ROLE restricted NOINHERIT IN ROLE reader",
        )
    } else {
        (
            "CREATE ROLE reader SUPERUSER",
            "CREATE ROLE restricted IN ROLE reader",
        )
    };
    for sql in [
        reader,
        restricted,
        "CREATE TABLE t(id int PRIMARY KEY)",
        "INSERT INTO t VALUES (7)",
    ] {
        simple(&mut stream, sql).await;
    }
    if cfg!(feature = "engine-rocksdb") {
        simple(&mut stream, "GRANT SELECT ON t TO reader").await;
    }

    for (sql, allowed) in [
        ("PREPARE s AS SELECT id FROM public.t WHERE id = 7", true),
        ("SET SESSION AUTHORIZATION restricted", false),
        ("SET ROLE reader", true),
        ("RESET ROLE", false),
        ("RESET SESSION AUTHORIZATION", true),
    ] {
        simple(&mut stream, sql).await;
        let frames = outcome(&mut stream, "s").await;
        let rows: Vec<_> = frames
            .iter()
            .filter(|(tag, _)| *tag == b'D')
            .map(|(_, body)| body.as_slice())
            .collect();
        let error = frames.iter().find(|(tag, _)| *tag == b'E');
        if allowed {
            assert!(error.is_none(), "{sql}: {frames:?}");
            assert_eq!(rows, [b"\0\x01\0\0\0\x017".as_slice()], "{sql}");
        } else {
            assert!(rows.is_empty(), "{sql}: {frames:?}");
            assert!(
                error.is_some_and(|(_, body)| body.windows(7).any(|field| field == b"C42501\0")),
                "{sql}: {frames:?}"
            );
        }
    }
    drop(stream);
    f.stop().await;
}
