//! GD-0082: replies to a pipelined extended-protocol batch leave the server in
//! one write, and a lone message still gets its reply without Sync.
//!
//! Message-by-message writes made each reply after the first wait for the
//! client's delayed ACK under Nagle (about 40 ms per query on Linux).
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
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use wire_support::{bind, cstr, execute, parse, pg_read};

struct Fixture {
    pg: Running,
    path: PathBuf,
    service: Arc<Service>,
}

impl Fixture {
    async fn new(test: &str) -> Self {
        Self::with_tls(test, None).await
    }

    async fn with_tls(test: &str, pem: Option<(String, String)>) -> Self {
        let path = std::env::temp_dir().join(format!(
            "pgwire-pipelined-{test}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin =
            Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::Legacy).unwrap();
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
        let mut config = PostgresqlConfig {
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
        if let Some((certificate, key)) = pem {
            let cert_path = path.join("server.pem");
            let key_path = path.join("server.key");
            std::fs::write(&cert_path, certificate).unwrap();
            std::fs::write(&key_path, key).unwrap();
            config.tls = TlsMode::Required;
            config.certificate = Some(cert_path.to_str().unwrap().into());
            config.private_key = Some(key_path.to_str().unwrap().into());
        }
        let pg = serve_postgresql(
            config.listen,
            service.clone(),
            Arc::new(config.build().unwrap()),
        )
        .await
        .unwrap();
        Self { pg, path, service }
    }

    async fn connect(&self) -> TcpStream {
        self.connect_key().await.0
    }

    async fn connect_key(&self) -> (TcpStream, (u32, u32)) {
        let mut key = None;
        let mut stream = TcpStream::connect(self.pg.address).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let mut startup = 196608u32.to_be_bytes().to_vec();
        for value in ["user", "owner", "database", "postgres", ""] {
            cstr(value, &mut startup);
        }
        stream.write_u32((startup.len() + 4) as u32).await.unwrap();
        stream.write_all(&startup).await.unwrap();
        loop {
            let (tag, body) = pg_read(&mut stream).await;
            assert_ne!(tag, b'E', "startup: {}", String::from_utf8_lossy(&body));
            if tag == b'K' {
                key = Some((
                    u32::from_be_bytes(body[..4].try_into().unwrap()),
                    u32::from_be_bytes(body[4..].try_into().unwrap()),
                ));
            }
            if tag == b'Z' {
                return (stream, key.unwrap());
            }
        }
    }

    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

fn frame(tag: u8, body: &[u8], out: &mut Vec<u8>) {
    out.push(tag);
    out.extend(((body.len() + 4) as u32).to_be_bytes());
    out.extend(body);
}

/// Splits whole backend messages into their tags; panics on a partial one.
fn tags(bytes: &[u8]) -> Vec<u8> {
    let mut tags = vec![];
    let mut rest = bytes;
    while !rest.is_empty() {
        assert!(rest.len() >= 5, "partial message header: {rest:?}");
        let n = u32::from_be_bytes(rest[1..5].try_into().unwrap()) as usize;
        assert!(rest.len() > n, "partial message body: {rest:?}");
        tags.push(rest[0]);
        rest = &rest[1 + n..];
    }
    tags
}

#[tokio::test]
async fn pipelined_extended_batch_replies_in_one_write() {
    let fixture = Fixture::new("batch").await;
    let mut stream = fixture.connect().await;
    let mut batch = vec![];
    frame(b'P', &parse("", "SELECT 1"), &mut batch);
    frame(b'B', &bind("", "", &[], false), &mut batch);
    frame(b'D', b"P\0", &mut batch);
    frame(b'E', &execute("", 0), &mut batch);
    frame(b'S', &[], &mut batch);
    for _ in 0..5 {
        stream.write_all(&batch).await.unwrap();
        // One read returns whatever the first segment carried. Replies written
        // one message at a time arrive as ParseComplete alone.
        let mut first = vec![0; 64 * 1024];
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut first))
            .await
            .expect("reply timeout")
            .unwrap();
        assert_eq!(tags(&first[..n]), b"12TDCZ");
    }
    fixture.stop().await;
}

#[tokio::test]
async fn lone_messages_are_answered_without_sync() {
    let fixture = Fixture::new("lone").await;
    let mut stream = fixture.connect().await;
    // Parse with nothing behind it: the reply is not held for a later Sync.
    let mut one = vec![];
    frame(b'P', &parse("s1", "SELECT 1"), &mut one);
    stream.write_all(&one).await.unwrap();
    assert_eq!(pg_read(&mut stream).await.0, b'1');
    // Bind followed by Flush: both replies arrive before any Sync.
    let mut flush = vec![];
    frame(b'B', &bind("", "s1", &[], false), &mut flush);
    frame(b'H', &[], &mut flush);
    stream.write_all(&flush).await.unwrap();
    assert_eq!(pg_read(&mut stream).await.0, b'2');
    let mut sync = vec![];
    frame(b'S', &[], &mut sync);
    stream.write_all(&sync).await.unwrap();
    assert_eq!(pg_read(&mut stream).await.0, b'Z');
    fixture.stop().await;
}

fn extended(sql: &str, out: &mut Vec<u8>) {
    frame(b'P', &parse("", sql), out);
    frame(b'B', &bind("", "", &[], false), out);
    frame(b'E', &execute("", 0), out);
}

async fn cycle(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut replies = vec![];
    loop {
        let reply = pg_read(stream).await;
        let done = reply.0 == b'Z';
        replies.push(reply);
        if done {
            return replies;
        }
    }
}
fn value(body: &[u8]) -> i32 {
    assert_eq!(&body[..2], &1u16.to_be_bytes());
    let n = i32::from_be_bytes(body[2..6].try_into().unwrap()) as usize;
    std::str::from_utf8(&body[6..6 + n])
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn hundred_cycles_share_blocking_hops() {
    let fixture = Fixture::new("hops").await;
    let mut stream = fixture.connect().await;
    let before = fixture.service.pgwire_blocking_tasks();
    let mut batch = vec![];
    for i in 0..100 {
        extended(&format!("SELECT {i}"), &mut batch);
        frame(b'S', &[], &mut batch);
    }
    stream.write_all(&batch).await.unwrap();
    for i in 0..100 {
        let replies = cycle(&mut stream).await;
        assert_eq!(replies.iter().map(|r| r.0).collect::<Vec<_>>(), b"12DCZ");
        assert_eq!(value(&replies[2].1), i);
    }
    let hops = fixture.service.pgwire_blocking_tasks() - before;
    println!("pipeline blocking tasks: {hops}");
    assert!(hops < 150, "blocking tasks: {hops}");
    fixture.stop().await;
}

#[tokio::test]
async fn streamed_middle_execute_preserves_reply_order() {
    let fixture = Fixture::new("stream-order").await;
    let mut stream = fixture.connect().await;
    let mut setup = vec![];
    let values = (0..600)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    frame(
        b'Q',
        format!("CREATE TABLE batch_rows (id INT); INSERT INTO batch_rows VALUES {values}\0")
            .as_bytes(),
        &mut setup,
    );
    stream.write_all(&setup).await.unwrap();
    assert!(!cycle(&mut stream).await.iter().any(|r| r.0 == b'E'));
    let mut batch = vec![];
    extended("SELECT -1", &mut batch);
    extended("SELECT id FROM batch_rows ORDER BY id", &mut batch);
    extended("SELECT 999", &mut batch);
    frame(b'S', &[], &mut batch);
    stream.write_all(&batch).await.unwrap();
    let replies = cycle(&mut stream).await;
    let mut expected = b"12DC12".to_vec();
    expected.extend(std::iter::repeat_n(b'D', 600));
    expected.extend(b"C12DCZ");
    assert_eq!(replies.iter().map(|r| r.0).collect::<Vec<_>>(), expected);
    assert_eq!(
        replies
            .iter()
            .filter(|r| r.0 == b'D')
            .map(|r| value(&r.1))
            .collect::<Vec<_>>(),
        std::iter::once(-1)
            .chain(0..600)
            .chain(std::iter::once(999))
            .collect::<Vec<_>>()
    );
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_middle_execute_drains_to_sync_and_recovers() {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    let fixture = Fixture::new("cancel-middle").await;
    let (mut stream, key) = fixture.connect_key().await;
    let (entered, wait) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, hold) = mpsc::channel();
    let hold = Mutex::new(hold);
    let calls = AtomicUsize::new(0);
    assert!(
        fixture
            .service
            .set_statement_gate(Arc::new(move || {
                if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    entered
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .send(giggles_core::Cancellation::current().unwrap())
                        .ok()
                        .unwrap();
                    hold.lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(8))
                        .unwrap();
                }
                Ok(())
            }))
            .is_ok()
    );
    let mut batch = vec![];
    extended("SELECT 1", &mut batch);
    extended("SELECT 2", &mut batch);
    extended("SELECT 3", &mut batch);
    frame(b'S', &[], &mut batch);
    stream.write_all(&batch).await.unwrap();
    let context = tokio::time::timeout(Duration::from_secs(8), wait)
        .await
        .unwrap()
        .unwrap();
    let mut cancel = TcpStream::connect(fixture.pg.address).await.unwrap();
    for word in [16, 80877102, key.0, key.1] {
        cancel.write_u32(word).await.unwrap();
    }
    cancel.flush().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(8), cancel.read_u8())
            .await
            .unwrap()
            .is_err()
    );
    assert!(context.check().is_err());
    release.send(()).unwrap();
    let replies = cycle(&mut stream).await;
    assert_eq!(replies.iter().map(|r| r.0).collect::<Vec<_>>(), b"12DC12EZ");
    assert!(
        replies
            .iter()
            .any(|r| r.0 == b'E' && r.1.windows(5).any(|w| w == b"57014"))
    );
    let mut next = vec![];
    extended("SELECT 4", &mut next);
    frame(b'S', &[], &mut next);
    stream.write_all(&next).await.unwrap();
    let replies = cycle(&mut stream).await;
    assert_eq!(replies.iter().map(|r| r.0).collect::<Vec<_>>(), b"12DCZ");
    assert_eq!(value(&replies[2].1), 4);
    fixture.stop().await;
}

#[tokio::test]
async fn repeated_and_fragmented_batches_and_flush_make_progress() {
    let fixture = Fixture::new("fragmented").await;
    let mut stream = fixture.connect().await;
    for fragmented in [false, false, true] {
        let mut batch = vec![];
        for i in 0..8 {
            extended(&format!("SELECT {i}"), &mut batch);
        }
        frame(b'S', &[], &mut batch);
        if fragmented {
            for part in batch.chunks(3) {
                stream.write_all(part).await.unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        } else {
            stream.write_all(&batch).await.unwrap();
        }
        let replies = tokio::time::timeout(Duration::from_secs(5), cycle(&mut stream))
            .await
            .unwrap();
        assert_eq!(
            replies.iter().map(|r| r.0).collect::<Vec<_>>(),
            [b"12DC".repeat(8), b"Z".to_vec()].concat()
        );
        assert_eq!(
            replies
                .iter()
                .filter(|r| r.0 == b'D')
                .map(|r| value(&r.1))
                .collect::<Vec<_>>(),
            (0..8).collect::<Vec<_>>()
        );
    }
    let mut batch = vec![];
    extended("SELECT 42", &mut batch);
    frame(b'H', &[], &mut batch);
    stream.write_all(&batch).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        for expected in b"12DC" {
            assert_eq!(pg_read(&mut stream).await.0, *expected);
        }
    })
    .await
    .unwrap();
    // Only send Sync after all Flush replies have arrived.
    let mut sync = vec![];
    frame(b'S', &[], &mut sync);
    stream.write_all(&sync).await.unwrap();
    assert_eq!(pg_read(&mut stream).await.0, b'Z');
    fixture.stop().await;
}

#[tokio::test]
async fn copy_waits_for_input_and_accepts_coalesced_input() {
    let fixture = Fixture::new("copy-buffer").await;
    let mut stream = fixture.connect().await;
    let mut setup = vec![];
    frame(b'Q', b"CREATE TABLE buffered_copy (id INT)\0", &mut setup);
    stream.write_all(&setup).await.unwrap();
    assert_eq!(cycle(&mut stream).await.last().unwrap().0, b'Z');
    for coalesced in [false, true] {
        let mut query = vec![];
        frame(b'Q', b"COPY buffered_copy FROM STDIN\0", &mut query);
        let mut data = vec![];
        frame(b'd', b"1\n", &mut data);
        frame(b'd', b"2\n", &mut data);
        frame(b'c', &[], &mut data);
        if coalesced {
            query.extend(data);
            stream.write_all(&query).await.unwrap();
        } else {
            stream.write_all(&query).await.unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), pg_read(&mut stream))
                    .await
                    .unwrap()
                    .0,
                b'G'
            );
            for part in data.chunks(2) {
                stream.write_all(part).await.unwrap();
            }
        }
        let replies = tokio::time::timeout(Duration::from_secs(5), cycle(&mut stream))
            .await
            .unwrap();
        assert_eq!(
            replies.iter().map(|r| r.0).collect::<Vec<_>>(),
            if coalesced {
                b"GCZ".as_slice()
            } else {
                b"CZ".as_slice()
            }
        );
    }
    let mut query = vec![];
    frame(b'Q', b"SELECT count(*) FROM buffered_copy\0", &mut query);
    stream.write_all(&query).await.unwrap();
    let replies = cycle(&mut stream).await;
    assert_eq!(value(&replies[1].1), 4);
    fixture.stop().await;
}

#[tokio::test]
async fn idle_role_fence_delivers_eof_without_input() {
    let fixture = Fixture::new("idle-fatal").await;
    let mut idle = fixture.connect().await;
    let mut admin = fixture.connect().await;
    let mut query = vec![];
    frame(b'Q', b"ALTER ROLE owner NOLOGIN\0", &mut query);
    admin.write_all(&query).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        // Binding loss intentionally closes silently, matching
        // write_fence_termination_network::terminated.

        assert_eq!(idle.read(&mut [0; 1]).await.unwrap(), 0);
    })
    .await
    .unwrap();
    fixture.stop().await;
}

#[tokio::test]
async fn ssl_request_and_client_hello_in_one_write() {
    use std::io::{Read, Write};
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let fixture = Fixture::with_tls(
        "coalesced-tls",
        Some((cert.cert.pem(), cert.key_pair.serialize_pem())),
    )
    .await;
    let mut socket = TcpStream::connect(fixture.pg.address).await.unwrap();
    let mut tls = rustls::ClientConnection::new(
        Arc::new(config),
        rustls::pki_types::ServerName::try_from("localhost").unwrap(),
    )
    .unwrap();
    let mut first = vec![0, 0, 0, 8, 4, 210, 22, 47];
    tls.write_tls(&mut first).unwrap();
    assert!(
        first.len() > 8,
        "ClientHello must share the SSLRequest write"
    );
    socket.write_all(&first).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        assert_eq!(socket.read_u8().await.unwrap(), b'S');
        let mut startup = 196608u32.to_be_bytes().to_vec();
        startup.extend(b"user\0owner\0database\0postgres\0\0");
        let mut request = ((startup.len() + 4) as u32).to_be_bytes().to_vec();
        request.extend(startup);
        frame(b'Q', b"SELECT 42\0", &mut request);
        tls.writer().write_all(&request).unwrap();
        let mut plain = vec![];
        let mut ready = 0;
        let mut saw_row = false;
        loop {
            let mut output = vec![];
            while tls.wants_write() {
                tls.write_tls(&mut output).unwrap();
            }
            socket.write_all(&output).await.unwrap();
            let mut input = [0; 16384];
            let n = socket.read(&mut input).await.unwrap();
            assert_ne!(n, 0);
            tls.read_tls(&mut &input[..n]).unwrap();
            tls.process_new_packets().unwrap();
            let mut chunk = [0; 16384];
            loop {
                match tls.reader().read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => plain.extend_from_slice(&chunk[..n]),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("TLS plaintext: {e}"),
                }
            }
            // Consume complete frames, retaining any partial frame for the next read.
            while plain.len() >= 5 {
                let n = u32::from_be_bytes(plain[1..5].try_into().unwrap()) as usize + 1;
                if plain.len() < n {
                    break;
                }
                assert_ne!(plain[0], b'E', "TLS query error: {plain:?}");
                if plain[0] == b'D' {
                    assert_eq!(value(&plain[5..n]), 42);
                    saw_row = true;
                    // The query must also deliver its final ReadyForQuery.
                }
                if plain[0] == b'Z' && plain[5] == b'I' {
                    // Startup and query each send ReadyForQuery.
                    ready += 1;
                    if ready == 2 {
                        assert!(saw_row);
                        assert!(!tls.is_handshaking());
                        return;
                    }
                }
                plain.drain(..n);
            }
        }
    })
    .await
    .unwrap();
    fixture.stop().await;
}
