//! E939 policy: PostgreSQL 18 returns 42883 for these public calls for every role.
//! GigglesDB first authorizes: failure is 42501 to avoid revealing routine existence;
//! only an authorized statement reaches routine binding and returns 42883.
use giggles_core::{Admin, Limits, Principal, TaskId, roles::NewRole};
use giggles_server::{
    Binding, Role, Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
    serve_pg, tls,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use rustls::{ClientConfig, pki_types::PrivatePkcs8KeyDer};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

struct Cert {
    cert: Certificate,
    key: KeyPair,
}
fn cert(ca: Option<&Cert>, server: bool) -> Cert {
    let key = KeyPair::generate().unwrap();
    let mut p = CertificateParams::new(vec!["localhost".into()]).unwrap();
    if ca.is_none() {
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    } else {
        p.extended_key_usages = vec![if server {
            ExtendedKeyUsagePurpose::ServerAuth
        } else {
            ExtendedKeyUsagePurpose::ClientAuth
        }];
    }
    let cert = if let Some(ca) = ca {
        p.signed_by(&key, &ca.cert, &ca.key).unwrap()
    } else {
        p.self_signed(&key).unwrap()
    };
    Cert { cert, key }
}

static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    pg: Running,
    restricted: Running,
    app: Arc<ClientConfig>,
    task: Arc<ClientConfig>,
    path: PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "qualified-routine-network-{}-{}-{}",
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
        let mut sql = admin.sql_session();
        for query in [
            "CREATE TABLE T (id BIGINT PRIMARY KEY, txt TEXT)",
            "CREATE TABLE U (id BIGINT PRIMARY KEY, txt TEXT)",
        ] {
            sql.execute(query, &[], None).unwrap();
        }
        drop(sql);
        let ca = cert(None, false);
        let server = cert(Some(&ca), true);
        let app = cert(Some(&ca), false);
        let task = cert(Some(&ca), false);
        let domain = admin.authority_metadata().unwrap().0.to_string();
        let bindings = vec![
            Binding {
                fingerprint: tls::fingerprint(app.cert.der()),
                role: Role::Application {
                    domain: domain.clone(),
                    tables: BTreeSet::from(["t".into()]),
                },
            },
            Binding {
                fingerprint: tls::fingerprint(task.cert.der()),
                role: Role::Task {
                    domain,
                    task: TaskId(1),
                    principal: Principal(2),
                },
            },
        ];
        let service = Service::new(admin, bindings, Clock::new(0, 3600).unwrap()).unwrap();
        let server_tls = tls::server(
            vec![server.cert.der().clone()],
            PrivatePkcs8KeyDer::from(server.key.serialize_der()).into(),
            vec![ca.cert.der().clone()],
        )
        .unwrap();
        let restricted = serve_pg(
            "127.0.0.1:0".parse().unwrap(),
            service.clone(),
            server_tls,
            16,
        )
        .await
        .unwrap();
        let client_tls = |c: &Cert| {
            tls::client(
                vec![c.cert.der().clone()],
                PrivatePkcs8KeyDer::from(c.key.serialize_der()).into(),
                vec![ca.cert.der().clone()],
            )
            .unwrap()
        };
        let app = client_tls(&app);
        let task = client_tls(&task);
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
        Self {
            pg,
            restricted,
            app,
            task,
            path,
        }
    }
    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        self.restricted.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

async fn connect(fixture: &Fixture) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(fixture.pg.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}
#[tokio::test]
async fn qualified_routines_simple_and_extended_database_role() {
    let fixture = Fixture::new().await;
    let client = connect(&fixture).await;
    for extended in [false, true] {
        for (sql, code) in [
            ("SELECT public.quote_ident('a')", "42883"),
            ("SELECT \"public\".abs(1)", "42883"),
            ("SELECT public.count(*)", "42883"),
            ("SELECT upper(public.lower('a'))", "42883"),
            ("SELECT nosuchfn(1)", "42883"),
            ("SELECT public.quote_ident('a' FROM", "42601"),
        ] {
            let error = if extended {
                client.prepare(sql).await.unwrap_err()
            } else {
                client.simple_query(sql).await.unwrap_err()
            };
            assert_eq!(
                error.code().unwrap().code(),
                code,
                "extended={extended}: {sql}: {error}"
            );
        }
        let builtin = "SELECT abs(1) FROM nosuchtable";
        let qualified = "SELECT public.abs(1) FROM nosuchtable";
        let errors = if extended {
            [
                client.prepare(builtin).await.unwrap_err(),
                client.prepare(qualified).await.unwrap_err(),
            ]
        } else {
            [
                client.simple_query(builtin).await.unwrap_err(),
                client.simple_query(qualified).await.unwrap_err(),
            ]
        };
        assert_eq!(
            errors[0].code().unwrap(),
            errors[1].code().unwrap(),
            "extended={extended}: {errors:?}"
        );
        if extended {
            let row = client
                .query_one("SELECT pg_catalog.quote_ident('a')", &[])
                .await
                .unwrap();
            assert_eq!(row.get::<_, String>(0), "a");
        } else {
            let rows = client
                .simple_query("SELECT pg_catalog.quote_ident('a')")
                .await
                .unwrap();
            assert!(rows.iter().any(|r| matches!(r, tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0) == Some("a"))));
        }
    }
    drop(client);
    fixture.stop().await;
}
trait Wire: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> Wire for T {}
async fn send(s: &mut dyn Wire, tag: u8, body: &[u8]) {
    s.write_u8(tag).await.unwrap();
    s.write_u32((body.len() + 4) as u32).await.unwrap();
    s.write_all(body).await.unwrap();
    s.flush().await.unwrap();
}
async fn read(s: &mut dyn Wire) -> (u8, Vec<u8>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let tag = s.read_u8().await.unwrap();
        let size = s.read_u32().await.unwrap();
        assert!((4..=8 * 1024 * 1024).contains(&size));
        let mut body = vec![0; size as usize - 4];
        s.read_exact(&mut body).await.unwrap();
        (tag, body)
    })
    .await
    .expect("wire response timeout")
}
async fn wire(f: &Fixture, session: &str) -> Box<dyn Wire> {
    let mut s: Box<dyn Wire> = if matches!(session, "Application" | "Task") {
        let mut socket = TcpStream::connect(f.restricted.address).await.unwrap();
        socket.write_u32(8).await.unwrap();
        socket.write_u32(80877103).await.unwrap();
        assert_eq!(socket.read_u8().await.unwrap(), b'S');
        let config = if session == "Application" {
            &f.app
        } else {
            &f.task
        };
        Box::new(
            tokio_rustls::TlsConnector::from(config.clone())
                .connect("localhost".try_into().unwrap(), socket)
                .await
                .unwrap(),
        )
    } else {
        Box::new(TcpStream::connect(f.pg.address).await.unwrap())
    };
    let user = match session {
        "Database reader" => "reader",
        "Database denied" => "denied",
        "Application" | "Task" => "giggles",
        _ => "owner",
    };
    let mut startup = 196608u32.to_be_bytes().to_vec();
    let database = if matches!(session, "Application" | "Task") {
        "giggles"
    } else {
        "postgres"
    };
    startup.extend(format!("user\0{user}\0database\0{database}\0\0").as_bytes());
    s.write_u32((startup.len() + 4) as u32).await.unwrap();
    s.write_all(&startup).await.unwrap();
    loop {
        let (tag, body) = read(&mut *s).await;
        assert_ne!(tag, b'E', "startup: {body:?}");
        if tag == b'Z' {
            break;
        }
    }
    s
}
fn sqlstate(body: &[u8]) -> String {
    body.split(|b| *b == 0)
        .find(|field| field.first() == Some(&b'C'))
        .map(|field| String::from_utf8(field[1..].to_vec()).unwrap())
        .expect("SQLSTATE field")
}
async fn observe(f: &Fixture, session: &str, sql: &str, extended: bool) -> (String, &'static str) {
    let mut s = wire(f, session).await;
    if !extended {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        send(&mut *s, b'Q', &body).await;
        let mut code = "00000".to_string();
        loop {
            let (tag, body) = read(&mut *s).await;
            if tag == b'E' {
                code = sqlstate(&body);
            }
            if tag == b'Z' {
                return (code, "Query");
            }
        }
    }
    let mut parse = vec![0];
    parse.extend(sql.as_bytes());
    parse.extend([0, 0, 0]);
    // PostgreSQL reports 42883 at Parse; these GigglesDB failures also occur at Parse.
    // Keep stages separate so a future later failure is visible as a
    // stage difference vs PG (not a blocker).
    for (stage, tag, body, complete) in [
        ("Parse", b'P', parse, b'1'),
        ("Describe(statement)", b'D', vec![b'S', 0], b'T'),
        ("Bind", b'B', vec![0, 0, 0, 0, 0, 0, 0, 0], b'2'),
        ("Execute", b'E', vec![0, 0, 0, 0, 0], b'C'),
    ] {
        send(&mut *s, tag, &body).await;
        send(&mut *s, b'H', &[]).await;
        loop {
            let (tag, body) = read(&mut *s).await;
            if tag == b'E' {
                return (sqlstate(&body), stage);
            }
            if tag == complete || (stage == "Describe(statement)" && tag == b'n') {
                break;
            }
        }
    }
    ("00000".into(), "Complete")
}
#[tokio::test]
async fn qualified_routines_authorization_and_wire_stages() {
    let fixture = Fixture::new().await;
    let owner = connect(&fixture).await;
    owner
        .simple_query("CREATE ROLE reader LOGIN")
        .await
        .unwrap();
    owner
        .simple_query("CREATE ROLE denied LOGIN")
        .await
        .unwrap();
    owner
        .simple_query("GRANT SELECT ON TABLE T TO reader")
        .await
        .unwrap();
    drop(owner);
    let mut mismatches = vec![];
    for session in [
        "Database owner",
        "Database reader",
        "Database denied",
        "Application",
        "Task",
    ] {
        for extended in [false, true] {
            let authorized = matches!(
                session,
                "Database owner" | "Database reader" | "Application"
            );
            let mut cases = vec![
                (
                    "SELECT public.abs(id) FROM T",
                    if authorized { "42883" } else { "42501" },
                ),
                (
                    "SELECT id FROM T WHERE public.quote_ident(txt) = 'a'",
                    if authorized { "42883" } else { "42501" },
                ),
            ];
            if session == "Database owner" {
                cases.extend([
                    ("SELECT public.quote_ident('a')", "42883"),
                    ("SELECT \"public\".abs(1)", "42883"),
                    ("SELECT public.count(*)", "42883"),
                    ("SELECT upper(public.lower('a'))", "42883"),
                    ("SELECT public.isfinite(date '2000-01-01')", "42883"),
                ]);
            }
            if matches!(session, "Application" | "Task") {
                cases.extend([
                    ("SELECT public.quote_ident('a')", "42501"),
                    ("SELECT public.isfinite(date '2000-01-01')", "42501"),
                    ("SELECT public.abs(id) FROM U", "42501"),
                ]);
            } else if session == "Database denied" {
                cases.extend([
                    ("SELECT public.abs(id) FROM U", "42501"),
                ]);
            }
            for (sql, expected) in cases {
                let (code, stage) = observe(&fixture, session, sql, extended).await;
                println!(
                    "WO8 session={session} protocol={} sql={sql:?} code={code} stage={stage}",
                    if extended { "extended" } else { "simple" }
                );
                if code != expected {
                    mismatches.push(format!(
                        "{session} extended={extended} {sql}: {code} expected {expected}"
                    ));
                }
                let expected_stage = if !extended { "Query" } else { "Parse" };
                if stage != expected_stage {
                    mismatches.push(format!(
                        "{session} {sql}: stage {stage} expected {expected_stage}"
                    ));
                }
            }
        }
    }
    // Successful control proves each separately flushed extended stage can complete.
    assert_eq!(
        observe(
            &fixture,
            "Database owner",
            "SELECT pg_catalog.quote_ident('a')",
            true
        )
        .await,
        ("00000".into(), "Complete")
    );
    fixture.stop().await;
    assert!(
        mismatches.is_empty(),
        "E939 matrix mismatches:\n{}",
        mismatches.join("\n")
    );
}
