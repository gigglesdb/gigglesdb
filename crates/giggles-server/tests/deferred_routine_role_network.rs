//! DD-0068 F9: real LOGIN Database roles reach deferred routine binding on pgwire.
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
        let root = std::env::temp_dir();
        let path = root.join(format!(
            "deferred-routine-role-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        for (name, superuser) in [("owner", true), ("probe", false)] {
            admin
                .roles()
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    superuser,
                    ..NewRole::default()
                })
                .unwrap();
        }
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let config = PostgresqlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: TlsMode::Disabled,
            certificate: None,
            private_key: None,
            client_ca: None,
            certificates: BTreeMap::new(),
            databases: vec!["postgres".into()],
            // Trust skips password negotiation, not LOGIN or Database-role authorization.
            loopback_trust: true,
            connections: 16,
        };
        let listener = config.build().unwrap();
        let pg = serve_postgresql(config.listen, service, Arc::new(listener))
            .await
            .unwrap();
        let fixture = Self { pg, path };
        let owner = fixture.client("owner").await;
        // The serial column supplies a denied sequence owned through this denied table.
        owner
            .simple_query("CREATE TABLE denied_table (id serial PRIMARY KEY, txt text)")
            .await
            .unwrap();
        drop(owner);
        fixture
    }

    async fn client(&self, user: &str) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(self.pg.address.port())
            .user(user)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    async fn wire(&self, user: &str) -> TcpStream {
        let mut stream = TcpStream::connect(self.pg.address).await.unwrap();
        let mut startup = 196608u32.to_be_bytes().to_vec();
        startup.extend(format!("user\0{user}\0database\0postgres\0\0").as_bytes());
        stream.write_u32((startup.len() + 4) as u32).await.unwrap();
        stream.write_all(&startup).await.unwrap();
        let mut identity = false;
        let mut superuser = false;
        loop {
            let (tag, body) = read(&mut stream).await;
            assert_ne!(tag, b'E', "startup: {body:?}");
            if tag == b'S' {
                identity |= body == format!("session_authorization\0{user}\0").as_bytes();
                superuser |= body
                    == format!(
                        "is_superuser\0{}\0",
                        if user == "owner" { "on" } else { "off" }
                    )
                    .as_bytes();
            }
            if tag == b'Z' {
                break;
            }
        }
        assert!(
            identity && superuser,
            "Database role startup identity: {user}"
        );
        stream
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
        let size = stream.read_u32().await.unwrap();
        assert!((4..=8 * 1024 * 1024).contains(&size));
        let mut body = vec![0; size as usize - 4];
        stream.read_exact(&mut body).await.unwrap();
        (tag, body)
    })
    .await
    .expect("wire response timeout")
}

fn field(body: &[u8], key: u8) -> Option<&str> {
    body.split(|byte| *byte == 0)
        .find(|part| part.first() == Some(&key))
        .map(|part| std::str::from_utf8(&part[1..]).unwrap())
}

fn assert_error(
    code: &str, message: &str, detail: Option<&str>, hint: Option<&str>,
    sql: &str, expected: &str, signature: Option<&str>,
) {
    assert_eq!(code, expected, "{sql}: {message}");
    if let Some(signature) = signature {
        assert_eq!(message, format!("function {signature} does not exist"), "{sql}");
        assert_eq!(hint, Some(
            "No function matches the given name and argument types. You might need to add explicit type casts."
        ), "{sql}");
    }
    // Only the exact PostgreSQL routine signature may include submitted identifiers.
    // All other fields still exclude SQL text, names and literal expressions.
    for text in [signature.is_none().then_some(message), detail, hint].into_iter().flatten() {
        let text = text.to_ascii_lowercase();
        for secret in [
            sql.to_ascii_lowercase(),
            "select".into(),
            "nosuchfn".into(),
            "nosuchagg".into(),
            "nosuchschema".into(),
            "pg_catalog".into(),
            "coalesce".into(),
            "lower".into(),
            "avg".into(),
            "count".into(),
            "nextval".into(),
            "information_schema".into(),
            "_pg_expandarray".into(),
            "pg_toast".into(),
            "denied_table".into(),
            "'a'".into(),
            "1/0".into(),
            "1,2".into(),
        ] {
            assert!(
                !text.contains(&secret),
                "diagnostic leaks {secret:?}: {text:?}; SQL={sql}"
            );
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Simple,
    Prepare,
    Portal,
    Statement,
}

async fn observe(
    fixture: &Fixture, user: &str, sql: &str, expected: &str,
    signature: Option<&str>, protocol: Protocol,
) {
    if matches!(protocol, Protocol::Simple | Protocol::Prepare) {
        let client = fixture.client(user).await;
        let error = match protocol {
            Protocol::Simple => client.simple_query(sql).await.unwrap_err(),
            Protocol::Prepare => client.prepare(sql).await.unwrap_err(),
            _ => unreachable!(),
        };
        let error = error.as_db_error().expect("server error");
        assert_error(
            error.code().code(),
            error.message(),
            error.detail(),
            error.hint(),
            sql,
            expected,
            signature,
        );
        println!(
            "F9 user={user} protocol={protocol:?} code={} stage={}",
            error.code().code(),
            if matches!(protocol, Protocol::Simple) {
                "application SQL"
            } else {
                "application prepare"
            }
        );
        return;
    }
    let mut stream = fixture.wire(user).await;
    let mut parse = vec![0];
    parse.extend(sql.as_bytes());
    parse.extend([0, 0, 0]);
    send(&mut stream, b'P', &parse).await;
    if matches!(protocol, Protocol::Portal) {
        send(&mut stream, b'B', &[0; 8]).await;
        send(&mut stream, b'D', &[b'P', 0]).await;
        send(&mut stream, b'E', &[0; 5]).await;
    } else {
        send(&mut stream, b'D', &[b'S', 0]).await;
    }
    send(&mut stream, b'S', &[]).await;
    let mut errors = 0;
    let mut stage = "Parse";
    loop {
        let (tag, body) = read(&mut stream).await;
        match tag {
            b'1' => {
                stage = if matches!(protocol, Protocol::Portal) {
                    "Bind"
                } else {
                    "Describe statement"
                }
            }
            b'2' => stage = "Describe portal/Execute",
            b'E' => {
                errors += 1;
                assert_error(
                    field(&body, b'C').expect("SQLSTATE"),
                    field(&body, b'M').expect("message"),
                    field(&body, b'D'),
                    field(&body, b'H'),
                    sql,
                    expected,
                    signature,
                );
                println!("F9 user={user} protocol={protocol:?} code={expected} stage={stage}");
                // The baseline and F9 both reject at Parse; do not claim a Bind failure.
                assert_eq!(stage, "Parse", "{protocol:?}: {sql}");
            }
            b'Z' => {
                assert_eq!(
                    errors, 1,
                    "{protocol:?}: {sql}: exactly one ErrorResponse before final ReadyForQuery"
                );
                assert_eq!(body, [b'I']);
                break;
            }
            _ => {}
        }
    }
}

async fn matrix(user: &str, cases: &[(&str, &str, Option<&str>)]) {
    let fixture = Fixture::new().await;
    for protocol in [
        Protocol::Simple,
        Protocol::Prepare,
        Protocol::Portal,
        Protocol::Statement,
    ] {
        for &(sql, expected, signature) in cases {
            observe(&fixture, user, sql, expected, signature, protocol).await;
        }
    }
    fixture.stop().await;
}

#[tokio::test]
async fn deferred_routines_nonsuperuser_four_protocols() {
    matrix(
        "probe",
        &[
            ("SELECT nosuchfn()", "42883", Some("nosuchfn()")),
            ("SELECT \"LOWER\"('a')", "42883", Some("LOWER(unknown)")),
            ("SELECT pg_catalog.nosuchfn()", "42883", Some("pg_catalog.nosuchfn()")),
            ("SELECT public.coalesce(1,2)", "42883", Some("public.coalesce(integer, integer)")),
            ("SELECT nosuchagg(1)", "42883", Some("nosuchagg(integer)")),
            ("SELECT avg(nosuchfn(1))", "42883", Some("nosuchfn(integer)")),
            ("SELECT count(DISTINCT nosuchfn(1))", "42883", Some("nosuchfn(integer)")),
            ("SELECT count(*),nosuchfn()", "42883", Some("nosuchfn()")),
            ("SELECT 1 WHERE nosuchfn(1)=1", "42883", Some("nosuchfn(integer)")),
            ("SELECT nosuchschema.lower('a')", "3F000", None),
            ("SELECT nosuchschema.lower(1/0)", "3F000", None),
            ("SELECT information_schema._pg_expandarray(1)", "0A000", None),
            ("SELECT pg_toast.lower('a')", "42501", None),
            // Deliberate fail-closed policy: PG 18 returns 42703 for the unresolved x.
            ("SELECT nosuchfn(x)", "42501", None),
            ("SELECT nosuchfn(x) FROM denied_table", "42501", None),
            ("SELECT nextval('denied_table_id_seq'),nosuchfn()", "42501", None),
        ],
    )
    .await;
}

#[tokio::test]
async fn deferred_routines_superuser_four_protocols() {
    matrix(
        "owner",
        &[
            ("SELECT nosuchfn()", "42883", Some("nosuchfn()")),
            ("SELECT nosuchschema.lower('a')", "3F000", None),
            ("SELECT pg_toast.lower('a')", "42883", Some("pg_toast.lower(unknown)")),
        ],
    )
    .await;
}
