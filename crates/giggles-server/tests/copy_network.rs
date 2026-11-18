//! Raw PostgreSQL-v3 COPY sequences qualified against PostgreSQL 18.6.
mod api_support;

use giggles_core::{Admin, CoreError, Error, Limits, roles::NewRole};
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
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Duration, timeout},
};
static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    server: Running,
    path: PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_engine(giggles_core::Engine::Legacy).await
    }
    async fn with_engine(engine: giggles_core::Engine) -> Self {
        let path = std::env::temp_dir().join(format!(
            "copy-network-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
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
        admin
            .roles()
            .create(NewRole {
                name: "denied".into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        let mut sql = admin.sql_session();
        sql.execute(
            if engine == giggles_core::Engine::Legacy {
                "CREATE TABLE t (i INT PRIMARY KEY, x TEXT CHECK (x <> 'bad'), n BIGINT)"
            } else {
                "CREATE TABLE t (i INT PRIMARY KEY, x TEXT, n BIGINT)"
            },
            &[],
            None,
        )
        .unwrap();
        drop(sql);
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
        let server = serve_postgresql(config.listen, service, Arc::new(listener))
            .await
            .unwrap();
        Self { server, path }
    }
    async fn connect(&self) -> TcpStream {
        self.connect_as("owner").await
    }
    async fn connect_as(&self, user: &str) -> TcpStream {
        self.connect_with_key(user).await.0
    }
    async fn connect_with_key(&self, user: &str) -> (TcpStream, Vec<u8>) {
        let mut s = TcpStream::connect(self.server.address).await.unwrap();
        let mut b = 196608i32.to_be_bytes().to_vec();
        b.extend(b"user\0");
        b.extend(user.as_bytes());
        b.extend(b"\0database\0postgres\0\0");
        s.write_all(&((b.len() + 4) as i32).to_be_bytes())
            .await
            .unwrap();
        s.write_all(&b).await.unwrap();
        let replies = read(&mut s).await;
        assert_eq!(replies.last().unwrap().0, b'Z');
        let key = replies
            .into_iter()
            .find(|(t, _)| *t == b'K')
            .expect("BackendKeyData")
            .1;
        assert_eq!(key.len(), 8);
        (s, key)
    }
    async fn stop(self) {
        self.server.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}
async fn send(s: &mut TcpStream, t: u8, b: &[u8]) {
    let mut out = vec![t];
    out.extend(((b.len() + 4) as i32).to_be_bytes());
    out.extend(b);
    s.write_all(&out).await.unwrap();
}
async fn read(s: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut rows = vec![];
    loop {
        let tag = match timeout(
            if rows.is_empty() {
                Duration::from_secs(5)
            } else {
                Duration::from_millis(100)
            },
            s.read_u8(),
        )
        .await
        {
            Ok(Ok(t)) => t,
            Err(_) => break,
            Ok(Err(e)) => panic!("read tag: {e}"),
        };
        let n = s.read_i32().await.unwrap();
        assert!(n >= 4);
        let mut b = vec![0; (n - 4) as usize];
        s.read_exact(&mut b).await.unwrap();
        rows.push((tag, b));
    }
    rows
}
fn tags(rows: &[(u8, Vec<u8>)]) -> Vec<u8> {
    rows.iter().map(|r| r.0).collect()
}
fn error(rows: &[(u8, Vec<u8>)], code: &str) {
    assert!(
        rows.iter()
            .any(|(t, b)| *t == b'E' && b.windows(6).any(|x| x == format!("C{code}").as_bytes())),
        "{rows:?}"
    );
}
async fn query(s: &mut TcpStream, q: &str) -> Vec<(u8, Vec<u8>)> {
    let mut b = q.as_bytes().to_vec();
    b.push(0);
    send(s, b'Q', &b).await;
    read(s).await
}
async fn silent(s: &mut TcpStream) {
    assert!(
        timeout(Duration::from_millis(100), s.read_u8())
            .await
            .is_err(),
        "unexpected response"
    );
}
async fn start(s: &mut TcpStream, q: &str) {
    let r = query(s, q).await;
    assert_eq!(tags(&r), b"G");
    let formats = if q.contains("binary") {
        vec![1, 0, 3, 0, 1, 0, 1, 0, 1]
    } else {
        vec![0, 0, 3, 0, 0, 0, 0, 0, 0]
    };
    assert_eq!(r[0].1, formats);
}
#[tokio::test]
async fn simple_copy_atomic_recovery_and_transaction_status() {
    let f = Fixture::new().await;
    let mut s = f.connect().await;
    start(&mut s, "COPY t FROM STDIN").await;
    send(&mut s, b'd', b"1\tone\t7\n").await;
    silent(&mut s).await;
    send(&mut s, b'H', b"").await;
    send(&mut s, b'S', b"").await;
    silent(&mut s).await;
    send(&mut s, b'c', b"").await;
    let r = read(&mut s).await;
    assert_eq!(tags(&r), b"CZ");
    assert_eq!(r[0].1, b"COPY 1\0");
    assert_eq!(r[1].1, b"I");
    for (data, code) in [
        (b"2\ttwo\t8\nx\tbad\t9\n".as_slice(), "22P02"),
        (b"2\tbad\t8\n", "23514"),
    ] {
        start(&mut s, "COPY t FROM STDIN").await;
        send(&mut s, b'd', data).await;
        let r = read(&mut s).await;
        assert_eq!(tags(&r), b"EZ");
        error(&r, code);
        send(&mut s, b'd', b"3\tignored\t9\n").await;
        send(&mut s, b'c', b"").await;
        silent(&mut s).await;
        let r = query(&mut s, "SELECT count(*) FROM t").await;
        assert_eq!(tags(&r), b"TDCZ");
        assert_eq!(r[1].1, b"\0\x01\0\0\0\x011");
    }
    start(&mut s, "COPY t FROM STDIN").await;
    send(&mut s, b'd', b"2\ttwo\t8\n").await;
    send(&mut s, b'f', b"cancel\0").await;
    let r = read(&mut s).await;
    assert_eq!(tags(&r), b"EZ");
    error(&r, "57014");
    let r = query(&mut s, "BEGIN").await;
    assert_eq!(r.last().unwrap().1, b"T");
    start(&mut s, "COPY t FROM STDIN").await;
    send(&mut s, b'd', b"8\trollback\t9\n").await;
    send(&mut s, b'c', b"").await;
    let r = read(&mut s).await;
    assert_eq!(tags(&r), b"CZ");
    assert_eq!(r.last().unwrap().1, b"T");
    assert_eq!(query(&mut s, "ROLLBACK").await.last().unwrap().1, b"I");
    let r = query(&mut s, "SELECT count(*) FROM t").await;
    assert_eq!(r[1].1, b"\0\x01\0\0\0\x011");
    let r = query(&mut s, "BEGIN").await;
    assert_eq!(r.last().unwrap().1, b"T");
    start(&mut s, "COPY t FROM STDIN").await;
    send(&mut s, b'd', b"x\tbad\t9\n").await;
    let r = read(&mut s).await;
    assert_eq!(tags(&r), b"EZ");
    assert_eq!(r.last().unwrap().1, b"E");
    send(&mut s, b'c', b"").await;
    silent(&mut s).await;
    assert_eq!(query(&mut s, "ROLLBACK").await.last().unwrap().1, b"I");
    drop(s);
    f.stop().await;
}
#[tokio::test]
async fn extended_describe_copy_flush_sync() {
    let f = Fixture::new().await;
    let mut s = f.connect().await;
    for mode in ["ok", "decode", "check", "fail"] {
        send(&mut s, b'P', b"\0COPY t FROM STDIN\0\0\0").await;
        send(&mut s, b'D', b"S\0").await;
        send(&mut s, b'B', b"\0\0\0\0\0\0\0\0").await;
        send(&mut s, b'E', b"\0\0\0\0\0").await;
        send(&mut s, b'H', b"").await;
        let r = read(&mut s).await;
        assert_eq!(tags(&r), b"1tn2G");
        assert_eq!(r[1].1, b"\0\0");
        assert!(r[2].1.is_empty());
        assert_eq!(r[4].1, b"\0\0\x03\0\0\0\0\0\0");
        send(&mut s, b'S', b"").await;
        silent(&mut s).await;
        if mode == "fail" {
            send(&mut s, b'f', b"cancel\0").await;
        } else {
            send(
                &mut s,
                b'd',
                match mode {
                    "decode" => b"x\tbad\t8\n",
                    "check" => b"2\tbad\t8\n",
                    _ => b"1\tone\t7\n",
                },
            )
            .await;
        }
        if mode == "ok" {
            silent(&mut s).await;
            send(&mut s, b'c', b"").await;
            silent(&mut s).await;
            send(&mut s, b'H', b"").await;
            let r = read(&mut s).await;
            assert_eq!(tags(&r), b"C");
            assert_eq!(r[0].1, b"COPY 1\0");
        } else {
            let r = read(&mut s).await;
            assert_eq!(tags(&r), b"E");
            error(
                &r,
                match mode {
                    "decode" => "22P02",
                    "check" => "23514",
                    _ => "57014",
                },
            );
            send(&mut s, b'd', b"9\tignored\t9\n").await;
            send(&mut s, b'f', b"ignored\0").await;
            send(&mut s, b'c', b"").await;
            send(&mut s, b'H', b"").await;
            silent(&mut s).await;
        }
        send(&mut s, b'S', b"").await;
        let r = read(&mut s).await;
        assert_eq!(tags(&r), b"Z");
        assert_eq!(r[0].1, b"I");
    }
    drop(s);
    f.stop().await;
}
#[tokio::test]
async fn binary_pgx_shape_without_trailer_and_disconnect() {
    let f = Fixture::new().await;
    let mut s = f.connect().await;
    start(&mut s, "COPY t FROM STDIN (FORMAT binary)").await;
    let mut bytes = b"PGCOPY\n\xff\r\n\0".to_vec();
    bytes.extend([0; 8]);
    bytes.extend(3i16.to_be_bytes());
    bytes.extend(4i32.to_be_bytes());
    bytes.extend(1i32.to_be_bytes());
    bytes.extend(3i32.to_be_bytes());
    bytes.extend(b"one");
    bytes.extend((-1i32).to_be_bytes());
    bytes.extend(3i16.to_be_bytes());
    bytes.extend(4i32.to_be_bytes());
    bytes.extend(2i32.to_be_bytes());
    bytes.extend((-1i32).to_be_bytes());
    bytes.extend(8i32.to_be_bytes());
    bytes.extend(42i64.to_be_bytes());
    for b in bytes {
        send(&mut s, b'd', &[b]).await;
    }
    send(&mut s, b'c', b"").await;
    let r = read(&mut s).await;
    assert_eq!(tags(&r), b"CZ");
    assert_eq!(r[0].1, b"COPY 2\0");
    start(&mut s, "COPY t FROM STDIN").await;
    send(&mut s, b'd', b"3\tthree\t7\n").await;
    drop(s);
    let mut s = f.connect().await;
    let r = query(&mut s, "SELECT count(*) FROM t").await;
    assert_eq!(r[1].1, b"\0\x01\0\0\0\x012");
    drop(s);
    f.stop().await;
}
#[tokio::test]
async fn copy_character_typmods_text_and_binary() {
    let f = Fixture::new().await;
    let mut s = f.connect().await;
    let r = query(
        &mut s,
        "CREATE TABLE chars (i INT PRIMARY KEY, v VARCHAR(5), c CHAR(3))",
    )
    .await;
    assert_eq!(tags(&r), b"CZ");
    for binary in [false, true] {
        for (id, v, c, overflow) in [
            (1i32, "abcde   ", "x", false),
            (2, "ok", "abc   ", false),
            (3, "abcdef", "x", true),
            (4, "ok", "abcd", true),
        ] {
            let id = id + if binary { 10 } else { 0 };
            start(
                &mut s,
                if binary {
                    "COPY chars FROM STDIN (FORMAT binary)"
                } else {
                    "COPY chars FROM STDIN"
                },
            )
            .await;
            let bytes = if binary {
                let mut bytes = b"PGCOPY\n\xff\r\n\0".to_vec();
                bytes.extend([0; 8]);
                bytes.extend(3i16.to_be_bytes());
                bytes.extend(4i32.to_be_bytes());
                bytes.extend(id.to_be_bytes());
                for value in [v, c] {
                    bytes.extend((value.len() as i32).to_be_bytes());
                    bytes.extend(value.as_bytes());
                }
                bytes.extend((-1i16).to_be_bytes());
                bytes
            } else {
                format!("{id}\t{v}\t{c}\n").into_bytes()
            };
            send(&mut s, b'd', &bytes).await;
            send(&mut s, b'c', b"").await;
            let r = read(&mut s).await;
            if overflow {
                assert_eq!(tags(&r), b"EZ");
                error(&r, "22001");
            } else {
                assert_eq!(tags(&r), b"CZ");
                assert_eq!(r[0].1, b"COPY 1\0");
            }
        }
    }
    let r = query(&mut s, "SELECT v,c FROM chars ORDER BY i").await;
    assert_eq!(tags(&r), b"TDDDDCZ");
    for (row, expected) in r[1..5].iter().zip([
        b"\0\x02\0\0\0\x05abcde\0\0\0\x03x  ".as_slice(),
        b"\0\x02\0\0\0\x02ok\0\0\0\x03abc".as_slice(),
        b"\0\x02\0\0\0\x05abcde\0\0\0\x03x  ".as_slice(),
        b"\0\x02\0\0\0\x02ok\0\0\0\x03abc".as_slice(),
    ]) {
        assert_eq!(row.1, expected);
    }
    drop(s);
    f.stop().await;
}
#[tokio::test]
async fn denied_copy_authorization_precedes_classification_and_data() {
    let f = Fixture::new().await;
    let mut s = f.connect_as("denied").await;
    for sql in [
        "COPY t FROM STDIN WITH (FORMAT nosuch)",
        "COPY t FROM STDIN",
    ] {
        let r = query(&mut s, sql).await;
        assert_eq!(tags(&r), b"EZ");
        error(&r, "42501");
        send(&mut s, b'd', b"bad\tbad\tbad\n").await;
        send(&mut s, b'c', b"").await;
        silent(&mut s).await;
    }
    drop(s);
    f.stop().await;
}
#[tokio::test]
async fn foreign_key_and_unique_validation_at_copy_done() {
    let f = Fixture::new().await;
    let mut s = f.connect().await;
    assert_eq!(
        tags(
            &query(
                &mut s,
                "CREATE TABLE f (i INT PRIMARY KEY, p INT REFERENCES f(i))"
            )
            .await
        ),
        b"CZ"
    );
    for (bytes, code) in [
        (b"1\t2\n2\t1\n".as_slice(), None),
        (b"3\t99\n", Some("23503")),
        (b"1\t2\n", Some("23505")),
    ] {
        let r = query(&mut s, "COPY f FROM STDIN").await;
        assert_eq!(tags(&r), b"G");
        assert_eq!(r[0].1, b"\0\0\x02\0\0\0\0");
        send(&mut s, b'd', bytes).await;
        silent(&mut s).await;
        send(&mut s, b'c', b"").await;
        let r = read(&mut s).await;
        if let Some(code) = code {
            assert_eq!(tags(&r), b"EZ");
            error(&r, code);
        } else {
            assert_eq!(tags(&r), b"CZ");
            assert_eq!(r[0].1, b"COPY 2\0");
        }
    }
    drop(s);
    f.stop().await;
}
#[tokio::test]
async fn cancel_idle_copy_reports_when_copy_done_arrives() {
    let f = Fixture::new().await;
    for extended in [false, true] {
        let (mut s, key) = f.connect_with_key("owner").await;
        if extended {
            send(&mut s, b'P', b"\0COPY t FROM STDIN\0\0\0").await;
            send(&mut s, b'B', b"\0\0\0\0\0\0\0\0").await;
            send(&mut s, b'E', b"\0\0\0\0\0").await;
            send(&mut s, b'H', b"").await;
            assert_eq!(tags(&read(&mut s).await), b"12G");
        } else {
            start(&mut s, "COPY t FROM STDIN").await;
        }
        send(&mut s, b'd', b"1\tuncommitted\t7\n").await;
        silent(&mut s).await;
        let mut cancel = TcpStream::connect(f.server.address).await.unwrap();
        let mut packet = 16i32.to_be_bytes().to_vec();
        packet.extend(80877102i32.to_be_bytes());
        packet.extend(key);
        cancel.write_all(&packet).await.unwrap();
        drop(cancel);
        // PostgreSQL 18.6 processes the pending cancellation when COPY input resumes.
        silent(&mut s).await;
        send(&mut s, b'c', b"").await;
        let r = read(&mut s).await;
        error(&r, "57014");
        assert_eq!(
            tags(&r),
            if extended {
                b"E".as_slice()
            } else {
                b"EZ".as_slice()
            }
        );
        if !extended {
            assert_eq!(r.last().unwrap().1, b"I");
        }
        if extended {
            send(&mut s, b'S', b"").await;
            let r = read(&mut s).await;
            assert_eq!(tags(&r), b"Z");
            assert_eq!(r[0].1, b"I");
        }
        let r = query(&mut s, "SELECT count(*) FROM t").await;
        assert_eq!(tags(&r), b"TDCZ");
        assert_eq!(r[1].1, b"\0\x01\0\0\0\x010");
        drop(s);
    }
    f.stop().await;
}

#[tokio::test]
async fn binary_garbage_after_trailer_is_atomic_bad_format() {
    for separate_frame in [false, true] {
        let f = Fixture::new().await;
        let mut s = f.connect().await;
        start(&mut s, "COPY t FROM STDIN (FORMAT binary)").await;
        let mut bytes = b"PGCOPY\n\xff\r\n\0".to_vec();
        bytes.extend([0; 8]);
        bytes.extend(3i16.to_be_bytes());
        bytes.extend(4i32.to_be_bytes());
        bytes.extend(1i32.to_be_bytes());
        bytes.extend((-1i32).to_be_bytes());
        bytes.extend((-1i32).to_be_bytes());
        bytes.extend((-1i16).to_be_bytes());
        if separate_frame {
            send(&mut s, b'd', &bytes).await;
            silent(&mut s).await;
            send(&mut s, b'd', b"garbage").await;
        } else {
            bytes.extend(b"garbage");
            send(&mut s, b'd', &bytes).await;
        }
        let r = read(&mut s).await;
        assert_eq!(tags(&r), b"EZ");
        error(&r, "22P04");
        send(&mut s, b'c', b"").await;
        silent(&mut s).await;
        let r = query(&mut s, "SELECT count(*) FROM t").await;
        assert_eq!(r[1].1, b"\0\x01\0\0\0\x010");
        drop(s);
        f.stop().await;
    }
}

#[tokio::test]
async fn copy_authority_precedes_classification_on_native_and_non_sql_surfaces() {
    let f = api_support::Fixture::new().await;
    f.admin
        .sql_session()
        .execute("CREATE TABLE t (i INT)", &[], None)
        .unwrap();
    let statements = [
        "COPY t FROM STDIN WITH (FORMAT nosuch)",
        "COPY nosuch_table FROM STDIN",
        "COPY t FROM STDIN",
    ];
    // The application is restricted to app; the task has no SQL authority.
    for binding in [&f.bindings[0], &f.bindings[2]] {
        let mut session = f.service.session(&binding.fingerprint).unwrap();
        for sql in statements {
            let result = session.execute_sql(sql, &[]);
            assert!(
                matches!(result, Err(CoreError::Policy(Error::Scope))),
                "native {:?}, {sql}: {result:?}",
                binding.role,
            );
            let response = session.handle(giggles_server::Request::Sql {
                sql: sql.into(),
                params: vec![],
            });
            assert_eq!(response.code, "42501", "native {sql}: {response:?}");
            assert!(response.sql.is_none(), "native {sql}: {response:?}");
            assert_eq!(session.handle(giggles_server::Request::Ping).code, "ok");
        }
    }
    let config = giggles_server::tls::server(
        vec![f.server.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(f.server.key.serialize_der()).into(),
        vec![f.ca.cert.der().clone()],
    )
    .unwrap();
    let native = giggles_server::serve(
        "127.0.0.1:0".parse().unwrap(),
        f.service.clone(),
        config,
        16,
    )
    .await
    .unwrap();
    for (cert, task) in [(&f.app, false), (&f.task, true)] {
        let mut config = (*f.client_config(cert, false)).clone();
        config.alpn_protocols.clear();
        let mut client =
            giggles_server::Client::connect(native.address, "localhost", Arc::new(config))
                .await
                .unwrap();
        for sql in statements {
            let response = client
                .call(&giggles_server::Request::Sql {
                    sql: sql.into(),
                    params: vec![],
                })
                .await
                .unwrap();
            assert_eq!(
                response.code, "42501",
                "native task={task}, {sql}: {response:?}"
            );
            assert!(response.sql.is_none(), "{response:?}");
            let usable = if task {
                giggles_server::Request::Status {
                    operation: giggles_core::OperationId(1),
                }
            } else {
                giggles_server::Request::Sql {
                    sql: "SELECT id FROM app".into(),
                    params: vec![],
                }
            };
            assert_eq!(
                client.call(&usable).await.unwrap().code,
                if task { "absent" } else { "ok" }
            );
        }
    }
    native.shutdown().await.unwrap();
    for cert in [&f.app, &f.task] {
        for h2 in [false, true] {
            let token = f.open_http(cert, h2).await;
            for sql in statements {
                let response = f
                    .exec_http(cert, &token, serde_json::json!({"sql":{"sql":sql}}), h2)
                    .await;
                assert_eq!(response["code"], "42501", "HTTP h2={h2}, {sql}: {response}");
                assert!(response.get("sql").is_none(), "{response}");
                let ping = f
                    .exec_http(cert, &token, serde_json::json!({"ping":{}}), h2)
                    .await;
                assert_eq!(ping["code"], "ok", "HTTP h2={h2}, after {sql}: {ping}");
            }
        }
    }
    let path = f.path.clone();
    f.stop().await;
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn copy_formats_and_savepoint_recovery_legacy_v2_parity() {
    let mut transcripts = Vec::new();
    for engine in [giggles_core::Engine::Legacy, giggles_core::Engine::V2] {
        let f = Fixture::with_engine(engine).await;
        let mut s = f.connect().await;
        let mut transcript = Vec::new();
        for format in ["text", "csv", "binary"] {
            transcript.push(query(&mut s, "BEGIN").await);
            transcript.push(query(&mut s, "SAVEPOINT copy_mark").await);
            start(&mut s, &format!("COPY t FROM STDIN (FORMAT {format})")).await;
            let bytes = match format {
                "text" => b"1\tone\t7\n2\t\\N\t8\n".to_vec(),
                "csv" => b"1,\"o,ne\",7\n2,,8\n".to_vec(),
                _ => {
                    let mut b = b"PGCOPY\n\xff\r\n\0".to_vec();
                    b.extend([0; 8]);
                    for id in [1i32, 2] {
                        b.extend(3i16.to_be_bytes());
                        b.extend(4i32.to_be_bytes());
                        b.extend(id.to_be_bytes());
                        b.extend((-1i32).to_be_bytes());
                        b.extend(8i32.to_be_bytes());
                        b.extend(7i64.to_be_bytes());
                    }
                    b.extend((-1i16).to_be_bytes());
                    b
                }
            };
            for chunk in bytes.chunks(3) {
                send(&mut s, b'd', chunk).await;
            }
            send(&mut s, b'c', b"").await;
            let r = read(&mut s).await;
            assert_eq!(tags(&r), b"CZ", "{engine:?} {format}: {r:?}");
            assert_eq!(r[0].1, b"COPY 2\0");
            transcript.push(r);
            transcript.push(query(&mut s, "SELECT count(*) FROM t").await);
            start(&mut s, "COPY t FROM STDIN").await;
            send(&mut s, b'd', b"3\tthree\t9\n1\tduplicate\t0\n").await;
            send(&mut s, b'c', b"").await;
            let r = read(&mut s).await;
            error(&r, "23505");
            assert_eq!(r.last().unwrap().1, b"E");
            // Error locations/messages may differ by engine; compare SQLSTATE/status.
            transcript.push(vec![(b'E', b"23505".to_vec()), r.last().unwrap().clone()]);
            transcript.push(query(&mut s, "ROLLBACK TO SAVEPOINT copy_mark").await);
            let r = query(&mut s, "SELECT count(*) FROM t").await;
            assert_eq!(r[1].1, b"\0\x01\0\0\0\x010");
            transcript.push(r);
            transcript.push(query(&mut s, "RELEASE SAVEPOINT copy_mark").await);
            transcript.push(query(&mut s, "COMMIT").await);
        }
        transcripts.push(transcript);
        drop(s);
        f.stop().await;
    }
    assert_eq!(transcripts[0], transcripts[1]);
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn bounded_copy_large_atomic_and_disconnect() {
    let f = Fixture::with_engine(giggles_core::Engine::V2).await;
    let mut s = f.connect().await;
    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, n INT)",
        "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT REFERENCES users(id), n INT)",
        "CREATE INDEX order_user ON orders(user_id)",
        "CREATE TABLE rejected (id INT PRIMARY KEY, email TEXT UNIQUE, n INT)",
        "CREATE TABLE orphans (id INT PRIMARY KEY, user_id INT REFERENCES users(id), n INT)",
    ] { assert_eq!(tags(&query(&mut s, sql).await), b"CZ", "{sql}"); }
    async fn load(s: &mut TcpStream, table: &str, count: usize, failure: &str) -> Vec<(u8, Vec<u8>)> {
        start(s, &format!("COPY {table} FROM STDIN")).await;
        for first in (1..=count).step_by(200) {
            let mut data = String::new();
            for id in first..=(first + 199).min(count) {
                let value = if table == "users" || table == "rejected" {
                    format!("u{}@example.test", if failure == "unique" && id == 49_999 { 1 } else { id })
                } else { (if failure == "fk" && id == count { 999_999 } else { (id - 1) % 50_000 + 1 }).to_string() };
                data.push_str(&format!("{id}\t{value}\t0\n"));
            }
            send(s, b'd', data.as_bytes()).await;
        }
        send(s, b'c', b"").await;
        timeout(Duration::from_secs(600), s.readable()).await.unwrap().unwrap();
        read(s).await
    }
    async fn count(s: &mut TcpStream, table: &str, expected: &str) {
        let r = query(s, &format!("SELECT count(*) FROM {table}")).await;
        assert_eq!(tags(&r), b"TDCZ", "{r:?}");
        assert_eq!(&r[1].1[6..], expected.as_bytes());
    }
    assert_eq!(load(&mut s, "users", 50_000, "").await[0].1, b"COPY 50000\0");
    let started = std::time::Instant::now();
    assert_eq!(load(&mut s, "orders", 200_000, "").await[0].1, b"COPY 200000\0");
    eprintln!("COPY orders: 200000 rows / {:.3}s = {:.0} rows/s", started.elapsed().as_secs_f64(), 200000.0 / started.elapsed().as_secs_f64());
    count(&mut s, "orders", "200000").await;
    // Existing UNIQUE/indexed destination already contains far more than 1,024 rows.
    start(&mut s, "COPY users FROM STDIN").await;
    send(&mut s, b'd', b"50001\tnew@example.test\t0\n").await;
    send(&mut s, b'c', b"").await;
    assert_eq!(read(&mut s).await[0].1, b"COPY 1\0");
    error(&load(&mut s, "rejected", 50_000, "unique").await, "23505");
    count(&mut s, "rejected", "0").await;
    error(&load(&mut s, "orphans", 2_000, "fk").await, "23503");
    count(&mut s, "orphans", "0").await;
    start(&mut s, "COPY rejected FROM STDIN").await;
    let data = (1..=400).map(|id| format!("{id}\tu{id}\t0\n")).collect::<String>();
    send(&mut s, b'd', data.as_bytes()).await;
    send(&mut s, b'f', b"cancel\0").await;
    error(&read(&mut s).await, "57014");
    count(&mut s, "rejected", "0").await;
    let mut abandoned = f.connect().await;
    start(&mut abandoned, "COPY rejected FROM STDIN").await;
    let data = (1..=400).map(|id| format!("{id}\tu{id}\t0\n")).collect::<String>();
    send(&mut abandoned, b'd', data.as_bytes()).await;
    silent(&mut abandoned).await;
    drop(abandoned);
    count(&mut s, "rejected", "0").await;
    start(&mut s, "COPY rejected FROM STDIN (FORMAT csv)").await;
    send(&mut s, b'd', b"1,\"a,b\",0\n2,\"\",0\n3,,0\n").await;
    send(&mut s, b'c', b"").await;
    assert_eq!(read(&mut s).await[0].1, b"COPY 3\0");
    let r = query(&mut s, "SELECT email FROM rejected ORDER BY id").await;
    assert_eq!(r[1].1, b"\0\x01\0\0\0\x03a,b");
    assert_eq!(r[2].1, b"\0\x01\0\0\0\0");
    assert_eq!(r[3].1, b"\0\x01\xff\xff\xff\xff");
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn missing_copy_columns_follow_supplied_field_conversion() {
    let mut engines = vec![giggles_core::Engine::Legacy];
    if cfg!(feature = "engine-rocksdb") { engines.push(giggles_core::Engine::V2); }
    for engine in engines {
        let f = Fixture::with_engine(engine).await;
        let mut s = f.connect().await;
        for csv in [false, true] {
            let sql = if csv { "COPY t FROM STDIN (FORMAT csv)" } else { "COPY t FROM STDIN" };
            for (data, code) in [
                ("x\n", "22P02"), ("1\n", "22P04"),
                (if csv { "x,bad,9,extra\n" } else { "x\tbad\t9\textra\n" }, "22P04"),
            ] {
                start(&mut s, sql).await;
                send(&mut s, b'd', data.as_bytes()).await;
                let r = read(&mut s).await;
                assert_eq!(tags(&r), b"EZ");
                error(&r, code);
                send(&mut s, b'c', b"").await;
                silent(&mut s).await;
            }
        }
        let r = query(&mut s, "SELECT count(*) FROM t").await;
        assert_eq!(r[1].1, b"\0\x01\0\0\0\x010");
        drop(s);
        f.stop().await;
    }
}

#[tokio::test]
async fn copy_to_option_profiles() {
    let f = Fixture::with_engine(if cfg!(feature = "engine-rocksdb") { giggles_core::Engine::V2 } else { giggles_core::Engine::Legacy }).await;
    let mut s = f.connect().await;
    query(&mut s, "INSERT INTO t VALUES (1,'a,b',1),(2,'q\"x',NULL),(3,'',3)").await;
    for (options, expected) in [
        ("WITH (FORMAT TEXT, HEADER, DELIMITER '|', NULL 'NUL')", "i|x|n\n1|a,b|1\n2|q\"x|NUL\n3||3\n"),
        ("WITH (FORMAT CSV, QUOTE '~', ESCAPE '!')", "1,~a,b~,1\n2,q\"x,\n3,~~,3\n"),
        ("WITH CSV HEADER FORCE QUOTE *", "i,x,n\n\"1\",\"a,b\",\"1\"\n\"2\",\"q\"\"x\",\n\"3\",\"\",\"3\"\n"),
        ("WITH (FORMAT CSV, HEADER, FORCE_QUOTE (x))", "i,x,n\n1,\"a,b\",1\n2,\"q\"\"x\",\n3,\"\",3\n"),
        ("WITH (FORMAT CSV, NULL 'NUL', DELIMITER '|', ENCODING 'UTF8')", "1|a,b|1\n2|\"q\"\"x\"|NUL\n3||3\n"),
        ("CSV FORCE QUOTE x", "1,\"a,b\",1\n2,\"q\"\"x\",\n3,\"\",3\n"),
        ("CSV QUOTE AS '~' ESCAPE AS '!'", "1,~a,b~,1\n2,q\"x,\n3,~~,3\n"),
    ] {
        let r = query(&mut s, &format!("COPY (SELECT * FROM t ORDER BY i) TO STDOUT {options}")).await;
        let data: Vec<u8> = r.iter().filter(|r| r.0 == b'd').flat_map(|r| r.1.iter().copied()).collect();
        assert_eq!(data, expected.as_bytes(), "{options}");
    }
    let r = query(&mut s, "COPY (SELECT 1) TO STDOUT BINARY").await;
    assert_eq!(r[0].0, b'H'); assert_eq!(r[0].1[0], 1);
    assert!(!r.iter().any(|r| r.0 == b'E'));
    query(&mut s, "INSERT INTO t VALUES (4,'q~!x',4)").await;
    let r = query(&mut s, "COPY (SELECT x FROM t WHERE i=4) TO STDOUT CSV QUOTE '~' ESCAPE '!'").await;
    let data: Vec<u8> = r.iter().filter(|r| r.0 == b'd').flat_map(|r| r.1.iter().copied()).collect();
    assert_eq!(data, b"~q!~!!x~\n");
    for (options, code) in [
        ("(DEFAULT 'x')", "0A000"), ("(FORCE_QUOTE *)", "0A000"),
        ("(FORMAT CSV, QUOTE 'xx')", "0A000"), ("(FORMAT CSV, ESCAPE 'xx')", "0A000"),
        ("(DELIMITER 'xx')", "0A000"), ("(FORMAT CSV, NULL ',')", "22023"),
        ("(FORMAT CSV, NULL '\"')", "22023"), ("(FORMAT CSV, HEADER MATCH)", "0A000"),
        ("(ENCODING 'bogus')", "22023"), ("(FORMAT CSV, FORCE_QUOTE (bogus))", "42703"),
        ("(FORMAT CSV, FORMAT CSV)", "42601"),
    ] {
        let r = query(&mut s, &format!("COPY t TO STDOUT {options}")).await;
        error(&r, code);
    }
    drop(s); f.stop().await;
}
