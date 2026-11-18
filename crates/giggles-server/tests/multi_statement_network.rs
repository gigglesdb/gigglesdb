use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Binding, Role, Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
    serve_pg, tls,
};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use rustls::{ClientConfig, pki_types::PrivatePkcs8KeyDer};
use std::{
    collections::{BTreeMap, BTreeSet},
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
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    service: Arc<Service>,
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "multi-statement-{}-{}-{}",
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
fn values(messages: &[SimpleQueryMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_owned()),
            _ => None,
        })
        .collect()
}
fn code(error: tokio_postgres::Error) -> String {
    error.code().unwrap().code().into()
}

#[tokio::test]
async fn ordered_result_sets_quotes_comments_and_empty_queries() {
    let f = Fixture::new().await;
    let c = f.client().await;
    let messages = c
        .simple_query("SELECT 1 AS a; SELECT 2 AS b")
        .await
        .unwrap();
    assert_eq!(values(&messages), ["1", "2"]);
    let descriptions: Vec<_> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::RowDescription(columns) => Some(columns[0].name()),
            _ => None,
        })
        .collect();
    assert_eq!(descriptions, ["a", "b"]);
    assert_eq!(
        messages
            .iter()
            .filter(|m| matches!(m, SimpleQueryMessage::CommandComplete(1)))
            .count(),
        2
    );
    for sql in [
        "SELECT 'a;b' AS x; SELECT 2 AS y",
        "SELECT E'a;b' AS x; SELECT 2 AS y",
        "SELECT 'a;b' AS \"x;y\"; /* ; /* nested */ */ SELECT 2 AS y -- ;\n;",
        ";;SELECT 'a;b' AS x;; SELECT 2 AS y;;",
    ] {
        assert_eq!(
            values(&c.simple_query(sql).await.unwrap()),
            ["a;b", "2"],
            "{sql}"
        );
    }
    let empty = c.simple_query(" ; ; -- ;\n /* ; */").await.unwrap();
    assert!(values(&empty).is_empty());
    assert!(
        empty
            .iter()
            .all(|m| matches!(m, SimpleQueryMessage::CommandComplete(0)))
    );
    // Splitting preserves dollar-quoted lexemes and their embedded semicolons.
    for literal in ["$$a;b$$", "$tag$a;b$tag$"] {
        let single = format!("SELECT {literal} AS x");
        assert_eq!(
            values(&c.simple_query(&single).await.unwrap()),
            ["a;b"]
        );
        assert_eq!(
            values(
                &c.simple_query(&format!("{single}; SELECT 2 AS y"))
                    .await
                    .unwrap()
            ),
            ["a;b", "2"]
        );
    }
    assert_eq!(
        values(
            &c.simple_query("SET application_name = 'batch'; SHOW application_name; SELECT 3 AS n")
                .await
                .unwrap()
        ),
        ["batch", "3"]
    );
    assert_eq!(
        values(
            &c.simple_query("SELECT typname FROM pg_catalog.pg_type LIMIT 1; SHOW TimeZone")
                .await
                .unwrap()
        )
        .len(),
        2
    );
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn ddl_dml_and_implicit_error_rollback_skip_later_statements() {
    let f = Fixture::new().await;
    let c = f.client().await;
    let result = c.simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT); INSERT INTO t VALUES (1, 'one'); SELECT body FROM t LIMIT 10").await.unwrap();
    assert_eq!(values(&result), ["one"]);
    assert_eq!(code(c.simple_query("INSERT INTO t VALUES (2, 'two'); INSERT INTO absent VALUES (9); INSERT INTO t VALUES (3, 'three')").await.unwrap_err()), "42P01");
    assert_eq!(
        values(&c.simple_query("SELECT id FROM t LIMIT 10").await.unwrap()),
        ["1"]
    );
    // Presentation settings follow the same implicit transaction rollback.
    assert!(c.simple_query("SET application_name = 'rolled back'; SELECT missing FROM t LIMIT 1; INSERT INTO t VALUES (4, 'four')").await.is_err());
    assert_eq!(
        values(&c.simple_query("SHOW application_name").await.unwrap()),
        [""]
    );
    assert_eq!(
        values(&c.simple_query("SELECT id FROM t LIMIT 10").await.unwrap()),
        ["1"]
    );
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn explicit_transaction_boundaries_and_recovery() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    c.simple_query("BEGIN; INSERT INTO t VALUES (1); COMMIT")
        .await
        .unwrap();
    assert_eq!(
        values(&c.simple_query("SELECT id FROM t LIMIT 10").await.unwrap()),
        ["1"]
    );
    c.simple_query("BEGIN").await.unwrap();
    c.simple_query("INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)")
        .await
        .unwrap();
    assert!(
        c.simple_query("INSERT INTO t VALUES (4); INSERT INTO absent VALUES (9); COMMIT")
            .await
            .is_err()
    );
    assert_eq!(
        code(
            c.simple_query("SELECT id FROM t LIMIT 10")
                .await
                .unwrap_err()
        ),
        "25P02"
    );
    c.simple_query("ROLLBACK").await.unwrap();
    assert_eq!(
        values(&c.simple_query("SELECT id FROM t LIMIT 10").await.unwrap()),
        ["1"]
    );
    c.simple_query("INSERT INTO t VALUES (5); COMMIT; INSERT INTO t VALUES (6); ROLLBACK")
        .await
        .unwrap();
    assert_eq!(
        values(&c.simple_query("SELECT id FROM t LIMIT 10").await.unwrap()),
        ["1", "5"]
    );
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn admin_statements_require_a_single_statement_message() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    for sql in [
        "INSERT INTO t VALUES (1); CREATE ROLE forbidden",
        "COMMIT; CREATE ROLE forbidden",
        "GRANT SELECT ON t TO owner; SELECT 1",
        "SELECT 1; DROP TABLE t",
    ] {
        assert_eq!(
            code(c.simple_query(sql).await.unwrap_err()),
            "25001",
            "{sql}"
        );
    }
    assert!(values(&c.simple_query("SELECT id FROM t LIMIT 10").await.unwrap()).is_empty());
    c.simple_query(";; CREATE ROLE allowed ;;").await.unwrap();
    c.simple_query("DROP TABLE t").await.unwrap();
    drop(c);
    f.stop().await;
}

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

#[tokio::test]
async fn agent_mtls_application_keeps_whole_query_rejection() {
    let path = std::env::temp_dir().join(format!(
        "multi-agent-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
    let ca = cert(None, false);
    let server = cert(Some(&ca), true);
    let app = cert(Some(&ca), false);
    let binding = Binding {
        fingerprint: tls::fingerprint(app.cert.der()),
        role: Role::Application {
            domain: admin.authority_metadata().unwrap().0.to_string(),
            tables: BTreeSet::from(["app".into()]),
        },
    };
    let service = Service::new(admin, vec![binding], Clock::new(0, 3600).unwrap()).unwrap();
    let server_tls = tls::server(
        vec![server.cert.der().clone()],
        PrivatePkcs8KeyDer::from(server.key.serialize_der()).into(),
        vec![ca.cert.der().clone()],
    )
    .unwrap();
    let agent = serve_pg(
        "127.0.0.1:0".parse().unwrap(),
        service.clone(),
        server_tls,
        8,
    )
    .await
    .unwrap();
    let client_tls = ClientConfig::builder()
        .with_root_certificates(tls::roots(vec![ca.cert.der().clone()]).unwrap())
        .with_client_auth_cert(
            vec![app.cert.der().clone()],
            PrivatePkcs8KeyDer::from(app.key.serialize_der()).into(),
        )
        .unwrap();
    let (c, conn) = Config::new()
        .host("localhost")
        .hostaddr(agent.address.ip())
        .port(agent.address.port())
        .user("giggles")
        .dbname("giggles")
        .ssl_mode(SslMode::Require)
        .connect(MakeRustlsConnect::new(client_tls))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    // The pre-existing agent profile permits bounded SET-only driver batches.
    let settings = c
        .simple_query("SET extra_float_digits = 3; SET application_name = 'agent'")
        .await
        .unwrap();
    assert_eq!(
        settings
            .iter()
            .filter(|m| matches!(m, SimpleQueryMessage::CommandComplete(0)))
            .count(),
        2
    );
    assert_eq!(
        values(&c.simple_query("SHOW application_name").await.unwrap()),
        ["agent"]
    );
    assert_eq!(
        values(&c.simple_query("SELECT 1 AS a").await.unwrap()),
        ["1"]
    );
    assert_eq!(
        code(
            c.simple_query("SELECT 1 AS a; SELECT 2 AS b")
                .await
                .unwrap_err()
        ),
        "42501"
    );
    assert_eq!(
        code(
            c.simple_query("SELECT 'a;b' AS x; SELECT 2 AS y")
                .await
                .unwrap_err()
        ),
        "42501"
    );
    assert_eq!(
        values(&c.simple_query("SELECT 2 AS b").await.unwrap()),
        ["2"]
    );
    drop(c);
    agent.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}

async fn wire_cycle(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut messages = Vec::new();
        loop {
            let tag = stream.read_u8().await.unwrap();
            let length = stream.read_u32().await.unwrap();
            assert!((4..65536).contains(&length));
            let mut body = vec![0; length as usize - 4];
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
async fn wire_query(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut body = Vec::new();
    giggles_server::pgwire::z(sql, &mut body);
    let mut packet = Vec::new();
    giggles_server::pgwire::frame(b'Q', &body, &mut packet).unwrap();
    stream.write_all(&packet).await.unwrap();
    wire_cycle(stream).await
}
fn tags(messages: &[(u8, Vec<u8>)]) -> Vec<u8> {
    messages.iter().map(|(tag, _)| *tag).collect()
}
#[tokio::test]
async fn wire_responses_keep_completions_and_release_portals_at_commit() {
    let f = Fixture::new().await;
    let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
    let body = b"\0\x03\0\0user\0owner\0database\0postgres\0\0";
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
    wire_cycle(&mut stream).await;
    assert_eq!(
        tags(&wire_query(&mut stream, "SELECT 1 AS a; SELECT 2 AS b").await),
        b"TDCTDCZ"
    );
    assert_eq!(tags(&wire_query(&mut stream, " ; ; ").await), b"IZ");
    wire_query(&mut stream, "CREATE TABLE t (id BIGINT PRIMARY KEY)").await;
    let failed = wire_query(
        &mut stream,
        "INSERT INTO t VALUES (1); INSERT INTO absent VALUES (2); INSERT INTO t VALUES (3)",
    )
    .await;
    assert_eq!(tags(&failed), b"CEZ");
    assert!(failed[1].1.split(|b| *b == 0).any(|v| v == b"C42P01"));
    assert_eq!(failed.last().unwrap().1, b"I");
    assert_eq!(
        tags(&wire_query(&mut stream, "SELECT id FROM t LIMIT 10").await),
        b"TCZ"
    );
    wire_query(&mut stream, "BEGIN").await;
    let mut packet = Vec::new();
    for (tag, body) in [
        (b'P', &b"s\0SELECT 1 AS n\0\0\0"[..]),
        (b'B', &b"p\0s\0\0\0\0\0\0\0"[..]),
        (b'E', &b"p\0\0\0\0\0"[..]),
        (b'S', &b""[..]),
    ] {
        giggles_server::pgwire::frame(tag, body, &mut packet).unwrap();
    }
    stream.write_all(&packet).await.unwrap();
    assert_eq!(tags(&wire_cycle(&mut stream).await), b"12DCZ");
    assert_eq!(
        tags(&wire_query(&mut stream, "COMMIT; BEGIN").await),
        b"CCZ"
    );
    packet.clear();
    giggles_server::pgwire::frame(b'E', b"p\0\0\0\0\0", &mut packet).unwrap();
    giggles_server::pgwire::frame(b'S', b"", &mut packet).unwrap();
    stream.write_all(&packet).await.unwrap();
    let missing = wire_cycle(&mut stream).await;
    assert_eq!(tags(&missing), b"EZ");
    assert!(missing[0].1.split(|b| *b == 0).any(|v| v == b"C34000"));
    wire_query(&mut stream, "ROLLBACK").await;
    drop(stream);
    f.stop().await;
}

#[tokio::test]
async fn aggregate_response_limit_rolls_back_before_publishing() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE big (id BIGINT PRIMARY KEY, body TEXT)")
        .await
        .unwrap();
    c.simple_query("CREATE TABLE marker (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    let text = "x".repeat(60_000);
    for id in 0..10i64 {
        c.execute("INSERT INTO big VALUES ($1, $2)", &[&id, &text])
            .await
            .unwrap();
    }
    let sql = format!(
        "INSERT INTO marker VALUES (1); {} INSERT INTO marker VALUES (2)",
        "SELECT body FROM big LIMIT 10;".repeat(16)
    );
    assert_eq!(code(c.simple_query(&sql).await.unwrap_err()), "54000");
    assert!(
        values(
            &c.simple_query("SELECT id FROM marker LIMIT 10")
                .await
                .unwrap()
        )
        .is_empty()
    );
    drop(c);
    f.stop().await;
}
