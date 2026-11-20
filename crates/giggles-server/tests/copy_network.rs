//! Raw PostgreSQL-v3 COPY sequences qualified against PostgreSQL 18.6.
mod api_support;

use giggles_core::{Admin, Limits, roles::NewRole};
#[cfg(feature = "legacy-task")]
use giggles_core::{CoreError, Error};
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

#[cfg(feature = "legacy-task")]
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

#[tokio::test]
async fn copy_option_parity() {
    let f = Fixture::new().await;
    let mut s = f.connect().await;
    for (sql, data) in [
        ("COPY t FROM STDIN WITH (FORMAT csv, HEADER MATCH, QUOTE '~', ESCAPE '!')", b"i,x,n\n1,~a!~b~,2\n".as_slice()),
        ("COPY t FROM STDIN CSV HEADER QUOTE '~' FORCE NOT NULL x", b"ignored\n2,,3\n"),
        ("COPY t FROM STDIN WITH (FORMAT csv, FORCE_NULL (x))", b"3,\"\",4\n"),
        ("COPY t FROM STDIN WITH (HEADER true, DELIMITER '|', NULL 'NIL')", b"ignored\n4|NIL|5\n"),
    ] {
        start(&mut s, sql).await;
        send(&mut s, b'd', data).await; send(&mut s, b'c', b"").await;
        assert_eq!(tags(&read(&mut s).await), b"CZ", "{sql}");
    }
    let rows = query(&mut s, "COPY t TO STDOUT WITH (FORMAT csv, HEADER, QUOTE '~', FORCE_QUOTE (x))").await;
    let data = rows.iter().filter(|r| r.0 == b'd').flat_map(|r| r.1.clone()).collect::<Vec<_>>();
    assert_eq!(data, b"i,x,n\n1,~a~~b~,2\n2,~~,3\n3,,4\n4,,5\n");

    for (sql, expected) in [
        ("COPY (SELECT 1 AS \"a,b\", 2 AS \"c\"\"d\") TO STDOUT WITH (FORMAT csv, HEADER, FORCE_QUOTE (\"a,b\"))", b"\"a,b\",\"c\"\"d\"\n\"1\",2\n".as_slice()),
        ("COPY (SELECT 1 AS \"a\tb\") TO STDOUT WITH (FORMAT text, HEADER)", b"a\\tb\n1\n"),
    ] {
        let rows = query(&mut s, sql).await;
        let data = rows.iter().filter(|r| r.0 == b'd').flat_map(|r| r.1.clone()).collect::<Vec<_>>();
        assert_eq!(data, expected, "{rows:?}");
    }
    for (sql, code, message) in [
        ("COPY t FROM STDIN WITH (FORMAT csv, QUOTE '~~')", "0A000", "COPY quote must be a single one-byte character"),
        ("COPY t TO STDOUT WITH (FORMAT binary, HEADER)", "0A000", "cannot specify HEADER in BINARY mode"),
        ("COPY t TO STDOUT WITH (HEADER 2)", "42601", "header requires a Boolean value or \"match\""),
        ("COPY (SELECT 1 AS a) TO STDOUT WITH (FORMAT csv, FORCE_QUOTE (nosuch))", "42703", "column \"nosuch\" does not exist"),
        ("COPY t TO STDOUT OIDS", "42601", "syntax error at or near \"OIDS\""),
        ("COPY t TO STDOUT WITH (bogus 1)", "42601", "option \"bogus\" not recognized"),
        ("COPY t TO STDOUT WITH (FORMAT xml)", "22023", "COPY format \"xml\" not recognized"),
        ("COPY t TO STDOUT WITH (HEADER, HEADER false)", "42601", "conflicting or redundant options"),
        ("COPY t TO STDOUT WITH (FORMAT csv, HEADER MATCH)", "0A000", "cannot use \"match\" with HEADER in COPY TO"),
        ("COPY t TO STDOUT WITH (QUOTE '~')", "0A000", "COPY QUOTE requires CSV mode"),
        ("COPY t TO STDOUT WITH (ESCAPE '~')", "0A000", "COPY ESCAPE requires CSV mode"),
        ("COPY t TO STDOUT WITH (FORMAT csv, ESCAPE '')", "0A000", "COPY escape must be a single one-byte character"),
        ("COPY t TO STDOUT WITH (DELIMITER '||')", "0A000", "COPY delimiter must be a single one-byte character"),
        ("COPY t TO STDOUT WITH (FORMAT csv, DELIMITER '~', QUOTE '~')", "22023", "COPY delimiter and quote must be different"),
        ("COPY t TO STDOUT WITH (DELIMITER '\\')", "22023", "COPY delimiter cannot be \"\\\""),
        ("COPY t TO STDOUT WITH (FORMAT csv, NULL 'a,b')", "22023", "COPY delimiter character must not appear in the NULL specification"),
        ("COPY t FROM STDIN WITH (FORMAT csv, FORCE_QUOTE (x))", "0A000", "COPY FORCE_QUOTE cannot be used with COPY FROM"),
        ("COPY t TO STDOUT WITH (FORMAT csv, FORCE_NULL (x))", "22023", "COPY FORCE_NULL cannot be used with COPY TO"),
        ("COPY t FROM STDIN WITH (FORMAT csv, FORCE_NULL (nosuch))", "42703", "column \"nosuch\" of relation \"t\" does not exist"),
        ("COPY t (i) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL (x))", "42P10", "FORCE_NOT_NULL column \"x\" not referenced by COPY"),
        ("COPY t (i) TO STDOUT WITH (FORMAT csv, FORCE_QUOTE (x))", "42P10", "FORCE_QUOTE column \"x\" not referenced by COPY"),
        ("COPY t TO STDOUT WITH (DEFAULT 'DEF')", "0A000", "COPY DEFAULT cannot be used with COPY TO"),
        ("COPY t FROM STDIN WITH (FORMAT binary, DEFAULT 'DEF')", "42601", "cannot specify DEFAULT in BINARY mode"),
        ("COPY t FROM STDIN WITH (FORMAT csv, DEFAULT 'x', NULL 'x')", "0A000", "NULL specification and DEFAULT specification cannot be the same"),
        ("COPY t FROM STDIN WITH (FORMAT csv, DEFAULT 'a,b')", "0A000", "COPY delimiter character must not appear in the DEFAULT specification"),
        ("COPY t FROM STDIN WITH (ON_ERROR bogus)", "22023", "COPY ON_ERROR \"bogus\" not recognized"),
        ("COPY t FROM STDIN WITH (REJECT_LIMIT 1)", "22023", "COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE"),
        ("COPY t FROM STDIN WITH (ON_ERROR ignore, REJECT_LIMIT 0)", "22023", "REJECT_LIMIT (0) must be greater than zero"),
        ("COPY t TO STDOUT WITH (FREEZE)", "22023", "COPY FREEZE cannot be used with COPY TO"),

    ] {
        let rows = query(&mut s, sql).await; error(&rows, code);
        assert!(rows.iter().any(|r| r.0 == b'E' && r.1.windows(message.len()).any(|b| b == message.as_bytes())), "{rows:?}");
    }

    for (sql, data, code, message) in [
        ("COPY t FROM STDIN WITH (FORMAT binary)", b"".as_slice(), "22P04", "COPY file signature not recognized"),
        ("COPY t FROM STDIN WITH (ON_ERROR stop, LOG_VERBOSITY default)", b"q\tx\t1\n", "22P02", "invalid input syntax for type integer: \"q\""),
    ] {
        start(&mut s, sql).await;
        if !data.is_empty() { send(&mut s, b'd', data).await; }
        if data.is_empty() { send(&mut s, b'c', b"").await; }
        let rows = read(&mut s).await; error(&rows, code);
        assert!(rows.iter().any(|r| r.1.windows(message.len()).any(|b| b == message.as_bytes())), "{rows:?}");
        if !data.is_empty() { send(&mut s, b'c', b"").await; }
    }
    start(&mut s, "COPY t FROM STDIN WITH (FORMAT csv, HEADER MATCH)").await;
    send(&mut s, b'd', b"i,bad,n\n").await;
    let rows = read(&mut s).await; error(&rows, "22P04");
    let message = b"column name mismatch in header line field 2: got \"bad\", expected \"x\"";
    assert!(rows.iter().any(|r| r.1.windows(message.len()).any(|b| b == message)), "{rows:?}");
    send(&mut s, b'c', b"").await;
    drop(s); f.stop().await;
}

fn w1_field(body: &[u8], field: u8) -> Option<String> {
    body.split(|b| *b == 0).find(|part| part.first() == Some(&field))
        .map(|part| String::from_utf8(part[1..].to_vec()).unwrap())
}
fn w1_data(rows: &[(u8, Vec<u8>)]) -> Vec<Vec<Option<String>>> {
    rows.iter().filter(|r| r.0 == b'D').map(|r| {
        let count = u16::from_be_bytes(r.1[..2].try_into().unwrap());
        let mut at = 2;
        (0..count).map(|_| {
            let length = i32::from_be_bytes(r.1[at..at + 4].try_into().unwrap()); at += 4;
            if length == -1 { None } else {
                let value = String::from_utf8(r.1[at..at + length as usize].to_vec()).unwrap(); at += length as usize; Some(value)
            }
        }).collect()
    }).collect()
}
fn w1_notices(rows: &[(u8, Vec<u8>)]) -> Vec<String> {
    rows.iter().filter(|r| r.0 == b'N').map(|r| {
        assert_eq!(w1_field(&r.1, b'C').as_deref(), Some("00000"));
        w1_field(&r.1, b'M').unwrap()
    }).collect()
}
async fn w1_copy(s: &mut TcpStream, sql: &str, data: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let r = query(s, sql).await;
    assert_eq!(tags(&r), b"G", "{r:?}");
    send(s, b'd', data).await;
    send(s, b'c', b"").await;
    let mut replies = read(s).await;
    while replies.last().is_some_and(|r| r.0 != b'Z') {
        let next = read(s).await; assert!(!next.is_empty(), "{replies:?}"); replies.extend(next);
    }
    replies
}
fn w1_tag(rows: &[(u8, Vec<u8>)], count: usize) {
    assert!(!rows.iter().any(|r| r.0 == b'E'), "{rows:?}");
    assert_eq!(rows.iter().find(|r| r.0 == b'C').unwrap().1, format!("COPY {count}\0").as_bytes());
}
#[tokio::test]
async fn w1_default_serial_raw_text_and_column_lists() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    query(&mut s, "CREATE TABLE w (id serial, a text DEFAULT 'da', b int DEFAULT 7)").await;
    let r = w1_copy(&mut s, "COPY w (id,a,b) FROM STDIN WITH (DEFAULT 'D')", b"D\tD\tD\n100\txD\t1\nD\ty\t2\n").await;
    w1_tag(&r, 3);
    let r = query(&mut s, "SELECT id,a,b FROM w ORDER BY id").await;
    assert_eq!(w1_data(&r), vec![vec![Some("1".to_owned()), Some("da".to_owned()), Some("7".to_owned())], vec![Some("2".to_owned()), Some("y".to_owned()), Some("2".to_owned())], vec![Some("100".to_owned()), Some("xD".to_owned()), Some("1".to_owned())]]);
    let r = w1_copy(&mut s, "COPY w (a,b) FROM STDIN WITH (DEFAULT '\\D')", b"\\D\t\\D\n").await; w1_tag(&r, 1);
    let r = query(&mut s, "SELECT id FROM w WHERE id=3").await;
    assert_eq!(r.iter().filter(|r| r.0 == b'D').count(), 1);
    f.stop().await;
}
#[tokio::test]
async fn w1_default_csv_quotes_header_force_and_identity() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    query(&mut s, "CREATE TABLE w (id int GENERATED ALWAYS AS IDENTITY, a text DEFAULT 'da')").await;
    let r = w1_copy(&mut s, "COPY w (id,a) FROM STDIN WITH (FORMAT csv, HEADER, DEFAULT 'DEF', FORCE_NOT_NULL (a), FORCE_NULL (a))", b"id,a\nDEF,DEF\nDEF,\"DEF\"\n").await;
    w1_tag(&r, 2);
    let r = query(&mut s, "SELECT a FROM w ORDER BY id").await;
    let data: Vec<_> = r.iter().filter(|r| r.0 == b'D').map(|r| r.1.clone()).collect();
    assert_eq!(data, [b"\0\x01\0\0\0\x02da".to_vec(), b"\0\x01\0\0\0\x03DEF".to_vec()]);
    f.stop().await;
}
#[tokio::test]
async fn w1_default_without_default_is_structural_before_conversion() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    let r = w1_copy(&mut s, "COPY t FROM STDIN WITH (DEFAULT 'D', ON_ERROR ignore)", b"bad\tD\t1\n").await;
    error(&r, "22P04");
    let e = &r.iter().find(|r| r.0 == b'E').unwrap().1;
    assert_eq!(w1_field(e, b'M').as_deref(), Some("unexpected default marker in COPY data"));
    assert_eq!(w1_field(e, b'D').as_deref(), Some("Column \"x\" has no default value."));
    assert!(w1_notices(&r).is_empty()); f.stop().await;
}
#[tokio::test]
async fn w1_ignore_types_and_transaction_survives() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    query(&mut s, "BEGIN").await;
    let r = w1_copy(&mut s, "COPY t FROM STDIN WITH (ON_ERROR ignore)", b"1\tx\t1\nnotint\tx\t1\n2\tx\t9999999999999999999999\n3\ty\t2\n").await;
    w1_tag(&r, 2); assert_eq!(tags(&r), b"NCZ");
    assert_eq!(w1_notices(&r), ["2 rows were skipped due to data type incompatibility"]);
    assert_eq!(r.last().unwrap().1, b"T");
    let r = query(&mut s, "SELECT count(*) FROM t").await; assert_eq!(r[1].1, b"\0\x01\0\0\0\x012");
    assert_eq!(query(&mut s, "COMMIT").await.last().unwrap().1, b"I"); f.stop().await;
}
#[tokio::test]
async fn w1_ignore_verbose_truncation_first_column_and_header() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    let long = "9".repeat(120);
    let data = format!("i,x,n\na,z,b\n{long},z,1\n2,z,b\n");
    let r = w1_copy(&mut s, "COPY t FROM STDIN WITH (FORMAT csv, HEADER, ON_ERROR ignore, LOG_VERBOSITY verbose)", data.as_bytes()).await;
    w1_tag(&r, 0); assert_eq!(tags(&r), b"NNNNCZ");
    assert_eq!(w1_notices(&r), [
        "skipping row due to data type incompatibility at line 2 for column \"i\": \"a\"".to_owned(),
        format!("skipping row due to data type incompatibility at line 3 for column \"i\": \"{}...\"", "9".repeat(100)),
        "skipping row due to data type incompatibility at line 4 for column \"n\": \"b\"".to_owned(),
        "3 rows were skipped due to data type incompatibility".to_owned(),
    ]); f.stop().await;
}
#[tokio::test]
async fn w1_ignore_domain_check_and_null_input() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    query(&mut s, "CREATE DOMAIN w_pos AS int CHECK (VALUE > 0) NOT NULL").await;
    query(&mut s, "CREATE TABLE w (p w_pos, t text)").await;
    let r = w1_copy(&mut s, "COPY w FROM STDIN WITH (ON_ERROR ignore, LOG_VERBOSITY verbose)", b"1\ta\n-1\tb\n\\N\tc\n").await;
    w1_tag(&r, 1);
    assert_eq!(w1_notices(&r), ["skipping row due to data type incompatibility at line 2 for column \"p\": \"-1\"", "skipping row due to data type incompatibility at line 3 for column \"p\": null input", "2 rows were skipped due to data type incompatibility"]);
    query(&mut s, "CREATE DOMAIN w_div AS int CHECK (1 / VALUE > 0)").await;
    query(&mut s, "CREATE TABLE w_math (i w_div)").await;
    let r = w1_copy(&mut s, "COPY w_math FROM STDIN WITH (ON_ERROR ignore, LOG_VERBOSITY verbose)", b"0\n1\n").await;
    error(&r, "22012"); assert!(w1_notices(&r).is_empty());
    let r = query(&mut s, "SELECT count(*) FROM w_math").await; assert_eq!(r[1].1, b"\0\x01\0\0\0\x010");
    f.stop().await;
}
#[tokio::test]
async fn w1_reject_limit_and_silent() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    let r = w1_copy(&mut s, "COPY t FROM STDIN WITH (ON_ERROR ignore, REJECT_LIMIT 2)", b"a\tx\t1\nb\tx\t1\n1\tx\t1\n").await;
    w1_tag(&r, 1); assert_eq!(w1_notices(&r), ["2 rows were skipped due to data type incompatibility"]);
    for verbosity in ["verbose", "silent"] {
        let r = w1_copy(&mut s, &format!("COPY t FROM STDIN WITH (ON_ERROR ignore, REJECT_LIMIT 1, LOG_VERBOSITY {verbosity})"), b"2\tx\t1\na\tx\t1\nb\tx\t1\n").await;
        error(&r, "22P02");
        assert_eq!(w1_field(&r.iter().find(|r| r.0 == b'E').unwrap().1, b'M').as_deref(), Some("skipped more than REJECT_LIMIT (1) rows due to data type incompatibility"));
        assert_eq!(w1_notices(&r).len(), if verbosity == "verbose" { 2 } else { 0 });
        let r = query(&mut s, "SELECT count(*) FROM t").await; assert_eq!(r[1].1, b"\0\x01\0\0\0\x011");
    } f.stop().await;
}
#[tokio::test]
async fn w1_ignore_keeps_constraints_and_structure_fatal() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    for (data, code) in [(b"1\tbad\t1\n".as_slice(), "23514"), (b"\\N\tx\t1\n", "23502"), (b"1\tx\t1\n1\ty\t2\n", "23505"), (b"1\tx\n", "22P04"), (b"1\tx\t1\textra\n", "22P04"), (b"1\t\xff\t1\n", "22021")] {
        let r = w1_copy(&mut s, "COPY t FROM STDIN WITH (ON_ERROR ignore)", data).await; error(&r, code); assert!(w1_notices(&r).is_empty());
        let r = query(&mut s, "SELECT count(*) FROM t").await; assert_eq!(r[1].1, b"\0\x01\0\0\0\x010");
    }
    for (tail, code) in [(b"1\tx\n".as_slice(), "22P04"), (b"1\tx\t1\textra\n", "22P04"), (b"1\t\xff\t1\n", "22021")] {
        let mut data = b"bad\tx\t1\n".to_vec(); data.extend_from_slice(tail);
        let r = w1_copy(&mut s, "COPY t FROM STDIN WITH (ON_ERROR ignore, LOG_VERBOSITY verbose)", &data).await;
        error(&r, code); assert_eq!(w1_notices(&r), ["skipping row due to data type incompatibility at line 1 for column \"i\": \"bad\""]);
    }
    f.stop().await;
}
#[tokio::test]
async fn w1_verbosity_without_ignore_is_accepted() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    for verbosity in ["verbose", "silent"] {
        let r = w1_copy(&mut s, &format!("COPY t FROM STDIN WITH (LOG_VERBOSITY {verbosity})"), b"1\tx\t1\n").await;
        if verbosity == "verbose" { w1_tag(&r, 1); } else { error(&r, "23505"); }
        assert!(w1_notices(&r).is_empty());
    } f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn w1_streaming_defaults_and_skips_cross_pages() {
    let f = Fixture::with_engine(giggles_core::Engine::V2).await; let mut s = f.connect().await;
    query(&mut s, "CREATE TABLE w (id serial PRIMARY KEY, a int DEFAULT 7)").await;
    let mut data = String::new();
    for i in 0..600 { data.push_str(if i % 100 == 0 { "D,bad\n" } else { "D,D\n" }); }
    let r = w1_copy(&mut s, "COPY w FROM STDIN WITH (FORMAT csv, DEFAULT 'D', ON_ERROR ignore)", data.as_bytes()).await;
    w1_tag(&r, 594); assert_eq!(w1_notices(&r), ["6 rows were skipped due to data type incompatibility"]);
    let r = query(&mut s, "SELECT count(*),max(id),min(a),max(a) FROM w").await;
    assert!(!r.iter().any(|r| r.0 == b'E'), "{r:?}");
    assert_eq!(r.iter().find(|r| r.0 == b'D').unwrap().1, b"\0\x04\0\0\0\x03594\0\0\0\x03600\0\0\0\x017\0\0\0\x017");
    let r = w1_copy(&mut s, "COPY w FROM STDIN WITH (FORMAT csv, DEFAULT 'D')", b"D,D\n\"unterminated\n").await;
    error(&r, "22P04");
    let r = w1_copy(&mut s, "COPY w FROM STDIN WITH (FORMAT csv, DEFAULT 'D')", b"D,D\n").await;
    w1_tag(&r, 1);
    let r = query(&mut s, "SELECT max(id) FROM w").await;
    assert_eq!(w1_data(&r), vec![vec![Some("602".to_owned())]]);
    f.stop().await;
}
#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn w1_streaming_reject_limit_counts_previous_pages() {
    let f = Fixture::with_engine(giggles_core::Engine::V2).await; let mut s = f.connect().await;
    let mut data = String::from("bad\tx\t1\n");
    for i in 1..200 { data.push_str(&format!("{i}\tx\t1\n")); }
    data.push_str("bad\tx\t1\n");
    let r = w1_copy(&mut s, "COPY t FROM STDIN WITH (ON_ERROR ignore, REJECT_LIMIT 1, LOG_VERBOSITY verbose)", data.as_bytes()).await;
    error(&r, "22P02");
    assert_eq!(w1_field(&r.iter().find(|r| r.0 == b'E').unwrap().1, b'M').as_deref(), Some("skipped more than REJECT_LIMIT (1) rows due to data type incompatibility"));
    assert_eq!(w1_notices(&r), ["skipping row due to data type incompatibility at line 1 for column \"i\": \"bad\"", "skipping row due to data type incompatibility at line 201 for column \"i\": \"bad\""]);
    let r = query(&mut s, "SELECT count(*) FROM t").await; assert_eq!(r[1].1, b"\0\x01\0\0\0\x010");
    f.stop().await;
}
#[tokio::test]
async fn w1_ignore_foreign_key_remains_fatal() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    query(&mut s, "CREATE TABLE w (id int PRIMARY KEY, parent int REFERENCES w(id))").await;
    let r = w1_copy(&mut s, "COPY w FROM STDIN WITH (ON_ERROR ignore)", b"1\t9\n").await;
    error(&r, "23503"); assert!(w1_notices(&r).is_empty());
    let r = query(&mut s, "SELECT count(*) FROM w").await; assert_eq!(r[1].1, b"\0\x01\0\0\0\x010"); f.stop().await;
}


#[tokio::test]
async fn w1_ignore_datetime_smallint_and_silent_success() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    query(&mut s, "CREATE TABLE w (i int, d date, s smallint)").await;
    let r = w1_copy(&mut s, "COPY w FROM STDIN WITH (ON_ERROR ignore)", b"1\t2020-01-01\t1\n99999999999\t2020-01-01\t1\n2\t2020-13-45\t1\n3\t2020-01-01\t70000\n4\t2020-01-01\t2\n").await;
    w1_tag(&r, 2); assert_eq!(w1_notices(&r), ["3 rows were skipped due to data type incompatibility"]);
    let r = w1_copy(&mut s, "COPY w FROM STDIN WITH (FORMAT csv, ON_ERROR ignore, LOG_VERBOSITY silent)", b"bad,bad,bad\n5,2020-01-02,3\n").await;
    w1_tag(&r, 1); assert_eq!(tags(&r), b"CZ");
    let r = query(&mut s, "SELECT i,d,s FROM w ORDER BY i").await;
    assert_eq!(w1_data(&r), vec![vec![Some("1".to_owned()),Some("2020-01-01".to_owned()),Some("1".to_owned())],vec![Some("4".to_owned()),Some("2020-01-01".to_owned()),Some("2".to_owned())],vec![Some("5".to_owned()),Some("2020-01-02".to_owned()),Some("3".to_owned())]]);
    let r = w1_copy(&mut s, "COPY w FROM STDIN WITH (ON_ERROR ignore, LOG_VERBOSITY verbose)", b"bad\n").await;
    w1_tag(&r, 0); assert_eq!(w1_notices(&r), ["skipping row due to data type incompatibility at line 1 for column \"i\": \"bad\"", "1 row was skipped due to data type incompatibility"]);
    f.stop().await;
}

#[tokio::test]
async fn w1_default_marker_sequence_privilege_is_required_only_for_markers() {
    #[cfg(feature = "engine-rocksdb")]
    let engines = [giggles_core::Engine::Legacy, giggles_core::Engine::V2];
    #[cfg(not(feature = "engine-rocksdb"))]
    let engines = [giggles_core::Engine::Legacy];
    for engine in engines {
    let f = Fixture::with_engine(engine).await; let mut owner = f.connect().await;
    query(&mut owner, "CREATE TABLE w (id serial, a text)").await;
    let r = query(&mut owner, "GRANT INSERT ON w TO denied").await;
    assert!(!r.iter().any(|r| r.0 == b'E'), "{r:?}");
    let mut denied = f.connect_as("denied").await;
    let r = w1_copy(&mut denied, "COPY w FROM STDIN WITH (FORMAT csv, DEFAULT 'D')", b"100,x\n").await;
    w1_tag(&r, 1);
    let r = w1_copy(&mut denied, "COPY w FROM STDIN WITH (FORMAT csv, DEFAULT 'D')", b"D,y\n").await;
    error(&r, "42501");
    let r = w1_copy(&mut owner, "COPY w FROM STDIN WITH (FORMAT csv, DEFAULT 'D')", b"D,y\n").await;
    w1_tag(&r, 1);
    let r = query(&mut owner, "SELECT id,a FROM w ORDER BY id").await;
    assert_eq!(w1_data(&r), vec![vec![Some("1".to_owned()),Some("y".to_owned())],vec![Some("100".to_owned()),Some("x".to_owned())]]);
    f.stop().await;
    }
}


#[tokio::test]
async fn w3_order_stop_conversion_before_omitted_defaults_and_constraints() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    query(&mut s, "CREATE TABLE worder (i INT CHECK (i > 0), id SERIAL)").await;
    let r = w1_copy(&mut s, "COPY worder (i) FROM STDIN", b"bad\n").await;
    error(&r, "22P02");
    let r = query(&mut s, "SELECT currval('worder_id_seq')").await;
    error(&r, "55000");
    let r = w1_copy(&mut s, "COPY worder (i) FROM STDIN", b"-1\nbad\n").await;
    error(&r, "23514");
    let r = query(&mut s, "SELECT currval('worder_id_seq')").await;
    assert_eq!(w1_data(&r), vec![vec![Some("1".into())]]);
    f.stop().await;
}

#[tokio::test]
async fn w3_order_ignore_supplied_markers_then_omitted_defaults() {
    let f = Fixture::new().await; let mut s = f.connect().await;
    query(&mut s, "CREATE TABLE wo2 (i INT, id SERIAL, j int DEFAULT nextval('wo2_id_seq'))").await;
    let r = w1_copy(&mut s, "COPY wo2 (i) FROM STDIN WITH (ON_ERROR ignore)", b"bad\n5\n").await;
    w1_tag(&r, 1);
    assert_eq!(w1_notices(&r), ["1 row was skipped due to data type incompatibility"]);
    let r = query(&mut s, "SELECT i,id,j FROM wo2 ORDER BY i").await;
    assert_eq!(w1_data(&r), vec![vec![Some("5".into()), Some("3".into()), Some("4".into())]]);
    let r = w1_copy(&mut s, "COPY wo2 (j, i) FROM STDIN WITH (ON_ERROR ignore, DEFAULT 'D')", b"D\tbad\nD\t7\n").await;
    w1_tag(&r, 1);
    assert_eq!(w1_notices(&r), ["1 row was skipped due to data type incompatibility"]);
    let r = query(&mut s, "SELECT i,id,j FROM wo2 ORDER BY i").await;
    assert_eq!(w1_data(&r), vec![vec![Some("5".into()), Some("3".into()), Some("4".into())], vec![Some("7".into()), Some("8".into()), Some("7".into())]]);
    // A marker after the first soft error must not consume a sequence value.
    let r = w1_copy(&mut s, "COPY wo2 (i,j) FROM STDIN WITH (ON_ERROR ignore, DEFAULT 'D')", b"bad\tD\n9\tD\n").await;
    w1_tag(&r, 1);
    let r = query(&mut s, "SELECT i,id,j FROM wo2 WHERE i = 9").await;
    assert_eq!(w1_data(&r), vec![vec![Some("9".into()), Some("11".into()), Some("10".into())]]);
    f.stop().await;
}
