//! DataGrip ctid table editor statement over the PostgreSQL extended protocol.
#[path = "wire_support/mod.rs"]
#[allow(dead_code)]
mod wire_support;
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use wire_support::{bind, cstr, execute, parse, pg_read, pg_send, pg_sync};

struct Fixture {
    pg: Running,
    path: PathBuf,
}

impl Fixture {
    async fn new(engine: Engine) -> Self {
        let path = std::env::temp_dir().join(format!(
            "truncate-wire-{engine:?}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&path).unwrap();
        let database = match engine {
            Engine::Legacy => path.join("db.redb"),
            Engine::V2 => path.join("db-v2"),
        };
        let admin = Admin::create_engine(database, Limits::default(), engine).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                superuser: true,
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "reader".into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        // Same loopback PostgreSQL listener as the existing authority/network tests.
        let config = PostgresqlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: TlsMode::Disabled,
            certificate: None,
            private_key: None,
            client_ca: None,
            certificates: BTreeMap::new(),
            databases: vec!["postgres".into()],
            loopback_trust: true,
            connections: 8,
        };
        let pg = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
            .await
            .unwrap();
        Self { pg, path }
    }

    async fn connect(&self, user: &str) -> TcpStream {
        let mut stream = TcpStream::connect(self.pg.address).await.unwrap();
        let mut startup = 196608u32.to_be_bytes().to_vec();
        for value in ["user", user, "database", "postgres", ""] {
            cstr(value, &mut startup);
        }
        stream.write_u32((startup.len() + 4) as u32).await.unwrap();
        stream.write_all(&startup).await.unwrap();
        loop {
            let (tag, body) = pg_read(&mut stream).await;
            assert_ne!(tag, b'E', "startup: {}", String::from_utf8_lossy(&body));
            if tag == b'Z' {
                return stream;
            }
        }
    }

    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

async fn simple(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut body = Vec::new();
    cstr(sql, &mut body);
    pg_send(stream, b'Q', &body).await;
    let mut frames = Vec::new();
    loop {
        let frame = pg_read(stream).await;
        let done = frame.0 == b'Z';
        frames.push(frame);
        if done {
            return frames;
        }
    }
}
fn success(frames: &[(u8, Vec<u8>)]) {
    assert!(
        !frames.iter().any(|f| f.0 == b'E'),
        "{:?}",
        frames
            .iter()
            .filter(|f| f.0 == b'E')
            .map(|f| String::from_utf8_lossy(&f.1))
            .collect::<Vec<_>>()
    );
}
fn error(frames: &[(u8, Vec<u8>)], code: &str) {
    let body = &frames
        .iter()
        .find(|f| f.0 == b'E')
        .expect("ErrorResponse")
        .1;
    if code == "0A000" {
        let message = b"cannot truncate a table referenced in a foreign key constraint";
        assert!(body.windows(message.len()).any(|bytes| bytes == message));
    }
    assert!(
        body.windows(code.len() + 2)
            .any(|w| w == format!("C{code}\0").as_bytes()),
        "{}",
        String::from_utf8_lossy(body)
    );
}
async fn exercise(engine: Engine) {
    let fixture = Fixture::new(engine).await;
    let mut stream = fixture.connect("owner").await;
    for sql in [
        "CREATE TABLE t(id integer PRIMARY KEY)",
        "INSERT INTO t VALUES(1)",
    ] {
        success(&simple(&mut stream, sql).await);
    }
    let frames = simple(&mut stream, "TRUNCATE TABLE t").await;
    success(&frames);
    assert!(
        frames
            .iter()
            .any(|f| f.0 == b'C' && f.1 == b"TRUNCATE TABLE\0")
    );
    success(&simple(&mut stream, "INSERT INTO t VALUES(1)").await);
    pg_send(
        &mut stream,
        b'P',
        &parse("clear", "TRUNCATE ONLY public.t CONTINUE IDENTITY RESTRICT"),
    )
    .await;
    pg_send(
        &mut stream,
        b'B',
        &bind("clear_portal", "clear", &[], false),
    )
    .await;
    pg_send(&mut stream, b'E', &execute("clear_portal", 0)).await;
    let frames = pg_sync(&mut stream).await;
    success(&frames);
    assert!(
        frames
            .iter()
            .any(|f| f.0 == b'C' && f.1 == b"TRUNCATE TABLE\0")
    );
    success(&simple(&mut stream, "BEGIN READ ONLY").await);
    error(&simple(&mut stream, "TRUNCATE t").await, "25006");
    success(&simple(&mut stream, "ROLLBACK").await);
    let mut reader = fixture.connect("reader").await;
    error(&simple(&mut reader, "TRUNCATE t").await, "42501");
    success(&simple(&mut stream, "GRANT TRUNCATE ON t TO reader").await);
    success(&simple(&mut reader, "TRUNCATE t").await);
    success(
        &simple(
            &mut stream,
            "CREATE TABLE child(id integer PRIMARY KEY, p integer REFERENCES t(id))",
        )
        .await,
    );
    // RESTRICT is an execution-time check: read-only mode wins even if a
    // referencing table exists, in both protocols and for a granted SQL role.
    for connection in [&mut stream, &mut reader] {
        success(&simple(connection, "BEGIN READ ONLY").await);
        error(&simple(connection, "TRUNCATE t").await, "25006");
        success(&simple(connection, "ROLLBACK").await);
    }
    success(&simple(&mut stream, "BEGIN READ ONLY").await);
    pg_send(&mut stream, b'P', &parse("readonly_fk", "TRUNCATE t")).await;
    pg_send(
        &mut stream,
        b'B',
        &bind("readonly_fk_portal", "readonly_fk", &[], false),
    )
    .await;
    pg_send(&mut stream, b'E', &execute("readonly_fk_portal", 0)).await;
    error(&pg_sync(&mut stream).await, "25006");
    success(&simple(&mut stream, "ROLLBACK").await);
    error(&simple(&mut reader, "TRUNCATE t CASCADE").await, "42501");
    success(&simple(&mut stream, "GRANT DELETE ON child TO reader").await);
    success(&simple(&mut reader, "TRUNCATE t CASCADE").await);
    error(&simple(&mut stream, "TRUNCATE t").await, "0A000");
    error(&simple(&mut stream, "TRUNCATE missing").await, "42P01");
    success(&simple(&mut stream, "CREATE SEQUENCE seq").await);
    error(&simple(&mut stream, "TRUNCATE seq").await, "42809");
    drop(reader);
    drop(stream);
    fixture.stop().await;
}
#[tokio::test]
async fn legacy() {
    exercise(Engine::Legacy).await;
}
#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn v2() {
    exercise(Engine::V2).await;
}
