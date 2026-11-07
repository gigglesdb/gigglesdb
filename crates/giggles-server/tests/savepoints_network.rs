use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_postgres::{Client, Config, NoTls, config::SslMode};
static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    service: Arc<Service>,
    pg: Running,
    path: std::path::PathBuf,
}

async fn cycle(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut messages = Vec::new();
        loop {
            let tag = stream.read_u8().await.unwrap();
            let len = stream.read_u32().await.unwrap();
            assert!((4..65536).contains(&len));
            let mut body = vec![0; len as usize - 4];
            stream.read_exact(&mut body).await.unwrap();
            messages.push((tag, body));
            if tag == b'Z' {
                return messages;
            }
        }
    })
    .await
    .unwrap()
}
async fn query(stream: &mut TcpStream, sql: &str, extended: bool) -> Vec<(u8, Vec<u8>)> {
    use giggles_server::pgwire::{frame, z};
    let mut packet = Vec::new();
    if extended {
        let mut parse = vec![0];
        z(sql, &mut parse);
        parse.extend(0u16.to_be_bytes());
        frame(b'P', &parse, &mut packet).unwrap();
        frame(b'B', &[0, 0, 0, 0, 0, 0, 0, 0], &mut packet).unwrap();
        frame(b'E', &[0, 0, 0, 0, 0], &mut packet).unwrap();
        frame(b'S', &[], &mut packet).unwrap();
    } else {
        let mut body = Vec::new();
        z(sql, &mut body);
        frame(b'Q', &body, &mut packet).unwrap();
    }
    stream.write_all(&packet).await.unwrap();
    cycle(stream).await
}
fn status(messages: &[(u8, Vec<u8>)]) -> u8 {
    messages.last().unwrap().1[0]
}
fn tag(messages: &[(u8, Vec<u8>)]) -> &str {
    let body = &messages.iter().find(|m| m.0 == b'C').unwrap().1;
    std::str::from_utf8(&body[..body.len() - 1]).unwrap()
}
fn error(messages: &[(u8, Vec<u8>)]) -> &str {
    let body = &messages.iter().find(|m| m.0 == b'E').unwrap().1;
    let at = body.iter().position(|b| *b == b'C').unwrap() + 1;
    std::str::from_utf8(&body[at..at + 5]).unwrap()
}
fn message(messages: &[(u8, Vec<u8>)]) -> &str {
    let body = &messages.iter().find(|m| m.0 == b'E').unwrap().1;
    let at = body.iter().position(|b| *b == b'M').unwrap() + 1;
    let end = body[at..].iter().position(|b| *b == 0).unwrap() + at;
    std::str::from_utf8(&body[at..end]).unwrap()
}
async fn wire(f: &Fixture) -> TcpStream {
    wire_as(f, "owner").await
}
async fn wire_as(f: &Fixture, user: &str) -> TcpStream {
    let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
    let mut body = 196608u32.to_be_bytes().to_vec();
    body.extend(b"user\0");
    body.extend(user.as_bytes());
    body.extend(b"\0database\0postgres\0\0");
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(&body).await.unwrap();
    assert_eq!(status(&cycle(&mut stream).await), b'I');
    stream
}
#[tokio::test]
async fn quoted_multibyte_names_share_truncated_settings_keys() {
    let f = Fixture::new().await;
    let c = f.client().await;
    let long = "🦀".repeat(21);
    let truncated = "🦀".repeat(15);
    let equivalent = format!("{truncated}🦀different");
    for extended in [false, true] {
        let commands = [
            "SET application_name='session'".to_owned(),
            "BEGIN".to_owned(),
            "SET LOCAL application_name='outer'".to_owned(),
            format!("SAVEPOINT \"{long}\""),
            "SET LOCAL application_name='inner'".to_owned(),
            format!("ROLLBACK TO SAVEPOINT \"{equivalent}\""),
        ];
        for sql in commands {
            if extended {
                c.execute(&sql, &[]).await.unwrap();
            } else {
                c.simple_query(&sql).await.unwrap();
            }
        }
        assert_eq!(
            c.query_one("SHOW application_name", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "outer"
        );
        let release = format!("RELEASE SAVEPOINT \"{truncated}\"");
        if extended {
            c.execute(&release, &[]).await.unwrap();
        } else {
            c.simple_query(&release).await.unwrap();
        }
        c.batch_execute("COMMIT").await.unwrap();
        assert_eq!(
            c.query_one("SHOW application_name", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "session"
        );
    }
    drop(c);
    f.stop().await;
}
#[tokio::test]
async fn ordinary_login_needs_no_grants_for_transaction_controls() {
    let f = Fixture::new().await;
    for extended in [false, true] {
        let mut s = wire_as(&f, "ordinary").await;
        for (sql, expected, ready) in [
            ("BEGIN", "BEGIN", b'T'),
            ("SAVEPOINT a", "SAVEPOINT", b'T'),
            ("ROLLBACK TRANSACTION TO SAVEPOINT a", "ROLLBACK", b'T'),
            ("RELEASE a", "RELEASE", b'T'),
            ("COMMIT", "COMMIT", b'I'),
            ("BEGIN", "BEGIN", b'T'),
            ("SAVEPOINT b", "SAVEPOINT", b'T'),
            ("ROLLBACK", "ROLLBACK", b'I'),
        ] {
            let r = query(&mut s, sql, extended).await;
            assert_eq!(tag(&r), expected);
            assert_eq!(status(&r), ready);
        }
        drop(s);
    }
    f.stop().await;
}
#[tokio::test]
async fn savepoint_tags_status_and_aborted_recovery_in_both_protocols() {
    let f = Fixture::new().await;
    for extended in [false, true] {
        let mut s = wire(&f).await;
        for (sql, name) in [
            ("SAVEPOINT a", "SAVEPOINT"),
            ("RELEASE a", "RELEASE SAVEPOINT"),
            ("ROLLBACK TO a", "ROLLBACK TO SAVEPOINT"),
        ] {
            let r = query(&mut s, sql, extended).await;
            assert_eq!(error(&r), "25P01");
            assert_eq!(status(&r), b'I');
            assert_eq!(
                message(&r),
                format!("{name} can only be used in transaction blocks")
            );
        }
        for (sql, expected) in [
            ("BEGIN", "BEGIN"),
            ("SAVEPOINT a", "SAVEPOINT"),
            ("SAVEPOINT a", "SAVEPOINT"),
            ("RELEASE SAVEPOINT a", "RELEASE"),
        ] {
            let r = query(&mut s, sql, extended).await;
            assert_eq!(tag(&r), expected);
            assert_eq!(status(&r), b'T');
        }
        let r = query(&mut s, "RELEASE absent", extended).await;
        assert_eq!(error(&r), "3B001");
        assert_eq!(status(&r), b'E');
        assert_eq!(message(&r), "savepoint \"absent\" does not exist");
        for sql in ["SAVEPOINT b", "RELEASE a"] {
            let r = query(&mut s, sql, extended).await;
            assert_eq!(error(&r), "25P02");
            assert_eq!(status(&r), b'E');
        }
        let r = query(&mut s, "ROLLBACK WORK TO SAVEPOINT a", extended).await;
        assert_eq!(tag(&r), "ROLLBACK");
        assert_eq!(status(&r), b'T');
        let r = query(&mut s, "COMMIT", extended).await;
        assert_eq!(tag(&r), "COMMIT");
        assert_eq!(status(&r), b'I');
        drop(s);
    }
    f.stop().await;
}
#[tokio::test]
async fn savepoints_reject_an_extended_implicit_transaction() {
    use giggles_server::pgwire::{frame, z};
    let f = Fixture::new().await;
    let mut s = wire(&f).await;
    let mut packet = Vec::new();
    for sql in ["SELECT 1", "/* control */ SAVEPOINT a"] {
        let mut parse = vec![0];
        z(sql, &mut parse);
        parse.extend(0u16.to_be_bytes());
        frame(b'P', &parse, &mut packet).unwrap();
        frame(b'B', &[0, 0, 0, 0, 0, 0, 0, 0], &mut packet).unwrap();
        frame(b'E', &[0, 0, 0, 0, 0], &mut packet).unwrap();
    }
    frame(b'S', &[], &mut packet).unwrap();
    s.write_all(&packet).await.unwrap();
    let r = cycle(&mut s).await;
    assert_eq!(error(&r), "25P01");
    assert_eq!(status(&r), b'I');
    drop(s);
    f.stop().await;
}
#[tokio::test]
async fn rollback_restores_transactional_and_local_settings() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.batch_execute("SET application_name='session'; BEGIN; SET LOCAL application_name='outer'; SAVEPOINT a; SET LOCAL application_name='inner'; SET search_path='inner'; ROLLBACK TO a").await.unwrap();
    assert_eq!(
        c.query_one("SHOW application_name", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "outer"
    );
    assert_eq!(
        c.query_one("SHOW search_path", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "\"$user\", public"
    );
    c.batch_execute("SET application_name='persistent'; SAVEPOINT b; SET LOCAL application_name='temporary'; RELEASE b; ROLLBACK TO a; COMMIT").await.unwrap();
    assert_eq!(
        c.query_one("SHOW application_name", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "session"
    );
    drop(c);
    f.stop().await;
}
impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "savepoints-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
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
        admin
            .roles()
            .create(NewRole {
                name: "ordinary".into(),
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
            connections: 8,
        };
        let listener = Arc::new(config.build().unwrap());
        let pg = serve_postgresql(config.listen, service.clone(), listener)
            .await
            .unwrap();
        Self { service, pg, path }
    }
    async fn client(&self) -> Client {
        let (client, connection) = Config::new()
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user("owner")
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        drop(self.service);
        // Listener shutdown waits for active connections to release the file.
        std::fs::remove_dir_all(self.path).unwrap();
    }
}
