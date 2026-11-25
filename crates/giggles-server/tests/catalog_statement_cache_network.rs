use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{fmt::Write as _, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{oneshot, watch},
};

const CLEANUP: &str = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name <> '_prisma_migrations' ORDER BY table_name";

type Frame = (u8, Vec<u8>);

async fn read_frame(socket: &mut TcpStream) -> Frame {
    let tag = socket.read_u8().await.unwrap();
    let len = socket.read_u32().await.unwrap() as usize;
    let mut body = vec![0; len - 4];
    socket.read_exact(&mut body).await.unwrap();
    (tag, body)
}

async fn receive(socket: &mut TcpStream) -> Vec<Frame> {
    let mut frames = Vec::new();
    loop {
        let (tag, body) = read_frame(socket).await;
        frames.push((tag, body));
        if tag == b'Z' {
            return frames;
        }
    }
}

async fn send(socket: &mut TcpStream, tag: u8, body: &[u8]) {
    let mut message = vec![tag];
    message.extend(((body.len() + 4) as u32).to_be_bytes());
    message.extend(body);
    socket.write_all(&message).await.unwrap();
}

fn error_fields(frames: &[Frame]) -> Option<(&str, &str)> {
    let (_, body) = frames.iter().find(|(tag, _)| *tag == b'E')?;
    let mut code = "";
    let mut message = "";
    for field in body
        .split(|&byte| byte == 0)
        .filter(|field| !field.is_empty())
    {
        match field[0] {
            b'C' => code = std::str::from_utf8(&field[1..]).unwrap(),
            b'M' => message = std::str::from_utf8(&field[1..]).unwrap(),
            _ => {}
        }
    }
    Some((code, message))
}

fn success(frames: &[Frame]) {
    assert_eq!(error_fields(frames), None);
}

async fn connect(address: SocketAddr) -> TcpStream {
    let mut socket = TcpStream::connect(address).await.unwrap();
    let startup = b"\0\x03\0\0user\0owner\0database\0postgres\0\0";
    socket.write_u32((startup.len() + 4) as u32).await.unwrap();
    socket.write_all(startup).await.unwrap();
    success(&receive(&mut socket).await);
    socket
}

async fn query(socket: &mut TcpStream, sql: &str) {
    send(socket, b'Q', format!("{sql}\0").as_bytes()).await;
    assert_eq!(error_fields(&receive(socket).await), None, "{sql}");
}

async fn parse(socket: &mut TcpStream, name: &str, sql: &str) {
    send(socket, b'P', format!("{name}\0{sql}\0\0\0").as_bytes()).await;
    send(socket, b'S', &[]).await;
    let frames = receive(socket).await;
    success(&frames);
    assert_eq!(frames[0].0, b'1');
}

async fn bind(socket: &mut TcpStream, name: &str) {
    // Unnamed portal, no parameters, one binary format for all result columns.
    let mut body = format!("\0{name}\0").into_bytes();
    body.extend(b"\0\0\0\0\0\x01\0\x01");
    send(socket, b'B', &body).await;
}

async fn execute(socket: &mut TcpStream) -> Vec<Frame> {
    send(socket, b'E', b"\0\0\0\0\0").await;
    send(socket, b'S', &[]).await;
    receive(socket).await
}

fn assert_cleanup_rows(frames: &[Frame], expected: &[String]) {
    success(frames);
    let names: Vec<&str> = frames
        .iter()
        .filter(|(tag, _)| *tag == b'D')
        .map(|(_, row)| {
            assert_eq!(&row[..2], &[0, 1]);
            let len = u32::from_be_bytes(row[2..6].try_into().unwrap()) as usize;
            assert_eq!(row.len(), 6 + len);
            std::str::from_utf8(&row[6..]).unwrap()
        })
        .collect();
    assert_eq!(names, expected);
}

async fn assert_cleanup(reader: &mut TcpStream, expected: &[String]) {
    bind(reader, "s1").await;
    let frames = execute(reader).await;
    success(&frames);
    assert_eq!(frames[0].0, b'2');
    assert_cleanup_rows(&frames, expected);
}

async fn bind_and_flush(reader: &mut TcpStream, name: &str) {
    bind(reader, name).await;
    send(reader, b'H', &[]).await;
    assert_eq!(read_frame(reader).await, (b'2', vec![]));
}

async fn start(engine: Engine) -> (Running, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "catalog-statement-cache-{}-{engine:?}",
        std::process::id()
    ));
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..Default::default()
        })
        .unwrap();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: Default::default(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 4,
    };
    let running = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    (running, path)
}

async fn catalog_statement_cache(engine: Engine) {
    let (running, path) = start(engine).await;
    let mut reader = connect(running.address).await;
    let mut writer = connect(running.address).await;
    query(&mut reader, r#"CREATE TABLE "User" (id int PRIMARY KEY); CREATE TABLE _prisma_migrations (id int PRIMARY KEY)"#).await;
    let mut expected = vec!["User".into()];
    parse(&mut reader, "s1", CLEANUP).await;
    assert_cleanup(&mut reader, &expected).await;

    query(&mut reader, r#"INSERT INTO "User" VALUES (1)"#).await;
    assert_cleanup(&mut reader, &expected).await;
    query(&mut writer, r#"INSERT INTO "User" VALUES (2)"#).await;
    assert_cleanup(&mut reader, &expected).await;
    query(&mut reader, r#"TRUNCATE "User" RESTART IDENTITY CASCADE"#).await;
    assert_cleanup(&mut reader, &expected).await;

    query(&mut writer, "CREATE TABLE another (id int PRIMARY KEY)").await;
    expected.push("another".into());
    assert_cleanup(&mut reader, &expected).await;

    bind_and_flush(&mut reader, "s1").await;
    query(&mut writer, "CREATE TABLE bound (id int PRIMARY KEY)").await;
    expected.push("bound".into());
    send(&mut reader, b'D', b"P\0").await;
    send(&mut reader, b'H', &[]).await;
    let (tag, description) = read_frame(&mut reader).await;
    assert_eq!(tag, b'T');
    assert!(description.ends_with(&1u16.to_be_bytes()));
    assert_cleanup_rows(&execute(&mut reader).await, &expected);

    let mixed =
        r#"SELECT u.*, c.relname FROM "User" u JOIN pg_catalog.pg_class c ON c.relname = 'User'"#;
    parse(&mut reader, "shape", mixed).await;
    bind(&mut reader, "shape").await;
    success(&execute(&mut reader).await);
    // A DDL commit after Bind also has to go through the result type check.
    bind_and_flush(&mut reader, "shape").await;
    query(&mut writer, r#"ALTER TABLE "User" ADD COLUMN body text"#).await;
    let message = Some(("0A000", "cached plan must not change result type"));
    assert_eq!(error_fields(&execute(&mut reader).await), message);
    send(&mut reader, b'D', b"Sshape\0").await;
    send(&mut reader, b'S', &[]).await;
    assert_eq!(error_fields(&receive(&mut reader).await), message);
    bind(&mut reader, "shape").await;
    assert_eq!(error_fields(&execute(&mut reader).await), message);

    let mut ddl = String::new();
    for n in 0..32 {
        let name = format!("catalog_cache_{n:02}");
        write!(ddl, "CREATE TABLE {name} (id int PRIMARY KEY);").unwrap();
        expected.push(name);
    }
    query(&mut writer, &ddl).await;

    let (started, ready) = oneshot::channel();
    let (stop, stopped) = watch::channel(false);
    let writer = tokio::spawn(async move {
        query(&mut writer, r#"INSERT INTO "User" VALUES (1, 'writer')"#).await;
        started.send(()).unwrap();
        let mut commits = 1;
        while !*stopped.borrow() {
            query(
                &mut writer,
                r#"UPDATE "User" SET body = CASE body WHEN 'writer' THEN 'updated' ELSE 'writer' END WHERE id = 1"#,
            )
            .await;
            commits += 1;
        }
        commits
    });
    ready.await.unwrap();
    parse(&mut reader, "concurrent_parse", CLEANUP).await;
    for _ in 0..60 {
        assert_cleanup(&mut reader, &expected).await;
    }
    stop.send(true).unwrap();
    assert!(writer.await.unwrap() > 1);

    #[cfg(feature = "engine-rocksdb")]
    if engine == Engine::V2 {
        query(&mut reader, "BEGIN; SAVEPOINT before_ddl").await;
        query(
            &mut reader,
            "CREATE TABLE private_catalog (id int PRIMARY KEY)",
        )
        .await;
        expected.push("private_catalog".into());
        assert_cleanup(&mut reader, &expected).await;
        query(&mut reader, "ROLLBACK TO before_ddl").await;
        expected.pop();
        assert_cleanup(&mut reader, &expected).await;
        query(&mut reader, "ROLLBACK").await;
    }

    drop(reader);
    running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_reuses_catalog_statements_after_writes_and_ddl() {
    catalog_statement_cache(Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_reuses_catalog_statements_after_writes_and_ddl() {
    catalog_statement_cache(Engine::V2).await;
}
