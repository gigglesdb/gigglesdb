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
            "transaction-modes-{}-{}-{}",
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

async fn agent_fixture(
    engine: giggles_core::Engine,
) -> (
    std::path::PathBuf,
    Arc<Service>,
    Running,
    ClientConfig,
    giggles_core::SqlSession,
) {
    let path = std::env::temp_dir().join(format!(
        "modes-agent-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap();
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
    let ddl = admin.sql_session();
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
    (path, service, agent, client_tls, ddl)
}

#[tokio::test]
async fn agent_transaction_surface_is_unchanged() {
    let (path, service, agent, client_tls, ddl) = agent_fixture(giggles_core::Engine::Legacy).await;
    drop(ddl);
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
    for sql in [
        "BEGIN WORK",
        "BEGIN READ ONLY",
        "BEGIN ISOLATION LEVEL READ COMMITTED",
        "COMMIT WORK",
        "END",
        "ABORT",
        "ROLLBACK TRANSACTION",
        "SET TRANSACTION READ ONLY",
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        "SET default_transaction_read_only=on",
        "SHOW transaction_read_only",
    ] {
        assert!(c.simple_query(sql).await.is_err(), "agent accepted {sql}");
        assert!(
            c.query(sql, &[]).await.is_err(),
            "agent extended accepted {sql}"
        );
    }
    // The base core parser already accepts bare START TRANSACTION for agents.
    c.simple_query("START TRANSACTION").await.unwrap();
    c.simple_query("ROLLBACK").await.unwrap();
    c.execute("START TRANSACTION", &[]).await.unwrap();
    c.execute("ROLLBACK", &[]).await.unwrap();
    for sql in ["BEGIN", "COMMIT", "BEGIN", "ROLLBACK"] {
        c.simple_query(sql).await.unwrap();
        c.execute(sql, &[]).await.unwrap();
    }
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

async fn wire_cycle(stream: &mut (impl tokio::io::AsyncRead + Unpin)) -> Vec<(u8, Vec<u8>)> {
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
async fn wire_query(
    stream: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
    sql: &str,
) -> Vec<(u8, Vec<u8>)> {
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
async fn isolation_levels_modes_and_control_synonyms_in_both_protocols() {
    let f = Fixture::new().await;
    let c = f.client().await;
    for level in [
        "SERIALIZABLE",
        "REPEATABLE READ",
        "READ COMMITTED",
        "READ UNCOMMITTED",
    ] {
        c.simple_query(&format!(
            "BEGIN TRANSACTION ISOLATION LEVEL {level}, READ WRITE, NOT DEFERRABLE"
        ))
        .await
        .unwrap();
        assert_eq!(values(&c.simple_query("SHOW transaction_isolation; SHOW default_transaction_isolation; SHOW transaction_read_only; SHOW default_transaction_read_only").await.unwrap()), ["serializable", "serializable", "off", "off"]);
        c.simple_query("END WORK").await.unwrap();
        c.execute(&format!("BEGIN ISOLATION LEVEL {level}"), &[])
            .await
            .unwrap();
        let rows = c.query("SHOW transaction_isolation", &[]).await.unwrap();
        // The legacy engine runs every level serializable and reports that (GD-0075 R5-75).
        assert_eq!(rows[0].get::<_, String>(0), "serializable");
        c.execute("COMMIT WORK", &[]).await.unwrap();
        c.execute(&format!("SET default_transaction_isolation='{level}'"), &[])
            .await
            .unwrap();
        assert_eq!(
            c.query("SHOW default_transaction_isolation", &[])
                .await
                .unwrap()[0]
                .get::<_, String>(0),
            "serializable"
        );
    }
    for (begin, end) in [
        ("BEGIN WORK", "COMMIT TRANSACTION"),
        ("START TRANSACTION DEFERRABLE", "END TRANSACTION"),
        ("BEGIN", "ABORT WORK"),
        ("START TRANSACTION", "ROLLBACK TRANSACTION"),
        ("BEGIN TRANSACTION", "ABORT TRANSACTION"),
    ] {
        c.simple_query(begin).await.unwrap();
        c.simple_query(end).await.unwrap();
        c.execute(begin, &[]).await.unwrap();
        c.execute(end, &[]).await.unwrap();
    }
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn read_only_rejects_every_write_and_keeps_reads_available() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    for sql in [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET id=2 WHERE id=1",
        "DELETE FROM t WHERE id=1",
        "CREATE TABLE forbidden (id BIGINT PRIMARY KEY)",
        "DROP TABLE t",
        "CREATE ROLE forbidden",
        "ALTER ROLE owner LOGIN",
        "DROP ROLE owner",
        "GRANT SELECT ON t TO owner",
        "REVOKE SELECT ON t FROM owner",
    ] {
        for extended in [false, true] {
            c.simple_query("START TRANSACTION READ ONLY").await.unwrap();
            assert_eq!(
                values(&c.simple_query("SELECT 1 AS n").await.unwrap()),
                ["1"]
            );
            assert_eq!(
                values(&c.simple_query("SHOW transaction_read_only").await.unwrap()),
                ["on"]
            );
            c.execute("SET application_name='allowed'", &[])
                .await
                .unwrap();
            let e = if extended {
                c.query(sql, &[]).await.unwrap_err()
            } else {
                c.simple_query(sql).await.unwrap_err()
            };
            assert_eq!(e.code().unwrap().code(), "25006", "{sql}");
            assert!(
                e.as_db_error()
                    .unwrap()
                    .message()
                    .starts_with("cannot execute ")
            );
            assert!(
                e.as_db_error()
                    .unwrap()
                    .message()
                    .ends_with(" in a read-only transaction")
            );
            c.simple_query("ABORT").await.unwrap();
        }
    }
    c.simple_query("BEGIN READ WRITE; INSERT INTO t VALUES (2); COMMIT")
        .await
        .unwrap();
    assert_eq!(
        values(&c.simple_query("SELECT id FROM t LIMIT 10").await.unwrap()),
        ["2"]
    );
    assert_eq!(
        code(
            c.simple_query("BEGIN READ ONLY; INSERT INTO t VALUES (3); COMMIT")
                .await
                .unwrap_err()
        ),
        "25006"
    );
    c.simple_query("ROLLBACK").await.unwrap();
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn set_transaction_before_queries_and_session_defaults() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    for extended in [false, true] {
        c.execute("BEGIN", &[]).await.unwrap();
        if extended {
            c.execute(
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY, DEFERRABLE",
                &[],
            )
            .await
            .unwrap();
        } else {
            c.simple_query("SET TRANSACTION READ ONLY").await.unwrap();
        }
        assert_eq!(
            c.query("SHOW transaction_isolation", &[]).await.unwrap()[0].get::<_, String>(0),
            "serializable"
        );
        assert_eq!(
            c.query("SHOW transaction_read_only", &[]).await.unwrap()[0].get::<_, String>(0),
            "on"
        );
        assert_eq!(
            code(
                c.execute("INSERT INTO t VALUES (1)", &[])
                    .await
                    .unwrap_err()
            ),
            "25006"
        );
        c.execute("ROLLBACK", &[]).await.unwrap();
        c.execute("BEGIN", &[]).await.unwrap();
        c.query("SELECT 1 AS n", &[]).await.unwrap();
        let e = if extended {
            c.execute("SET TRANSACTION READ ONLY", &[])
                .await
                .unwrap_err()
        } else {
            c.simple_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .await
                .unwrap_err()
        };
        assert_eq!(
            e.as_db_error().unwrap().message(),
            "SET TRANSACTION ISOLATION LEVEL must be called before any query"
        );
        assert_eq!(code(e), "25001");
        c.execute("ROLLBACK", &[]).await.unwrap();
    }
    for extended in [false, true] {
        let sql = "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY, NOT DEFERRABLE";
        if extended {
            c.execute(sql, &[]).await.unwrap();
        } else {
            c.simple_query(sql).await.unwrap();
        }
        assert_eq!(
            c.query("SHOW default_transaction_read_only", &[])
                .await
                .unwrap()[0]
                .get::<_, String>(0),
            "on"
        );
        c.execute("BEGIN", &[]).await.unwrap();
        assert_eq!(
            values(&c.simple_query("SHOW transaction_read_only").await.unwrap()),
            ["on"]
        );
        assert_eq!(
            code(
                c.execute("INSERT INTO t VALUES (1)", &[])
                    .await
                    .unwrap_err()
            ),
            "25006"
        );
        c.execute("ROLLBACK", &[]).await.unwrap();
        c.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE", &[])
            .await
            .unwrap();
        c.simple_query("BEGIN; INSERT INTO t VALUES (1); ROLLBACK")
            .await
            .unwrap();
    }
    c.execute("SET default_transaction_read_only=on", &[])
        .await
        .unwrap();
    assert_eq!(
        code(
            c.simple_query("INSERT INTO t VALUES (1)")
                .await
                .unwrap_err()
        ),
        "25006"
    );
    c.simple_query("BEGIN READ WRITE; INSERT INTO t VALUES (1); ROLLBACK")
        .await
        .unwrap();
    c.execute("RESET default_transaction_read_only", &[])
        .await
        .unwrap();
    c.simple_query("BEGIN; SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY; ROLLBACK")
        .await
        .unwrap();
    assert_eq!(
        values(
            &c.simple_query("SHOW default_transaction_read_only")
                .await
                .unwrap()
        ),
        ["off"]
    );
    c.simple_query("BEGIN; SELECT 1 AS n").await.unwrap();
    assert_eq!(
        code(
            c.simple_query("SET TRANSACTION READ WRITE")
                .await
                .unwrap_err()
        ),
        "25001"
    );
    c.simple_query("COMMIT WORK").await.unwrap(); // An aborted COMMIT is a ROLLBACK.
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn set_transaction_outside_a_block_warns_and_does_nothing() {
    let f = Fixture::new().await;
    let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
    let body = b"\0\x03\0\0user\0owner\0database\0postgres\0\0";
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
    wire_cycle(&mut stream).await;
    let messages = wire_query(&mut stream, "SET TRANSACTION READ ONLY").await;
    assert_eq!(tags(&messages), b"NCZ");
    assert!(messages[0].1.split(|b| *b == 0).any(|v| v == b"C25P01"));
    assert_eq!(messages.last().unwrap().1, b"I");
    let mut packet = Vec::new();
    for (tag, body) in [
        (b'P', &b"\0SET TRANSACTION READ ONLY\0\0\0"[..]),
        (b'B', &b"\0\0\0\0\0\0\0\0"[..]),
        (b'E', &b"\0\0\0\0\0"[..]),
        (b'S', &b""[..]),
    ] {
        giggles_server::pgwire::frame(tag, body, &mut packet).unwrap();
    }
    stream.write_all(&packet).await.unwrap();
    assert_eq!(tags(&wire_cycle(&mut stream).await), b"12NCZ");
    wire_query(&mut stream, "BEGIN").await;
    let shown = wire_query(&mut stream, "SHOW transaction_read_only").await;
    assert!(
        shown
            .iter()
            .any(|(tag, body)| *tag == b'D' && body.ends_with(b"off"))
    );
    wire_query(&mut stream, "ROLLBACK").await;
    drop(stream);
    f.stop().await;
}

// Check each extended-protocol stage independently: Parse/Describe must not
// create a transaction that changes a control plan's eventual refusal.
#[tokio::test]
async fn agent_extended_control_stages_preserve_refusals_after_catalog_change() {
    #[cfg(feature = "engine-rocksdb")]
    let engine = giggles_core::Engine::V2;
    #[cfg(not(feature = "engine-rocksdb"))]
    let engine = giggles_core::Engine::Legacy;
    let (path, service, agent, client_tls, mut ddl) = agent_fixture(engine).await;
    let mut tcp = TcpStream::connect(agent.address).await.unwrap();
    tcp.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await.unwrap();
    assert_eq!(tcp.read_u8().await.unwrap(), b'S');
    let mut stream = tokio_rustls::TlsConnector::from(Arc::new(client_tls))
        .connect("localhost".try_into().unwrap(), tcp)
        .await
        .unwrap();
    let startup = b"\0\x03\0\0user\0giggles\0database\0giggles\0\0";
    stream.write_u32((startup.len() + 4) as u32).await.unwrap();
    stream.write_all(startup).await.unwrap();
    wire_cycle(&mut stream).await;

    // A profile refusal at Parse discards subsequent Bind/Describe/Execute
    // messages through Sync; none may turn rejected syntax into a portal.
    let mut packet = Vec::new();
    for (tag, body) in [
        (b'P', &b"bad\0BEGIN WORK\0\0\0"[..]),
        (b'B', &b"p\0bad\0\0\0\0\0\0\0"[..]),
        (b'D', &b"Pp\0"[..]),
        (b'E', &b"p\0\0\0\0\0"[..]),
        (b'S', &b""[..]),
    ] {
        giggles_server::pgwire::frame(tag, body, &mut packet).unwrap();
    }
    stream.write_all(&packet).await.unwrap();
    assert_eq!(tags(&wire_cycle(&mut stream).await), b"EZ");

    // Core accepts this spelling at Parse; the agent profile historically
    // refuses it at Execute with 25000. Catalog refresh must preserve that.
    // Legacy plans retain their epoch fence; catalog-driven re-preparation is V2-only.
    let refresh_points: &[&str] = match engine {
        giggles_core::Engine::V2 => &["none", "describe", "bind"],
        giggles_core::Engine::Legacy => &["none"],
    };
    for &refresh_at in refresh_points {
        let name = format!("s_{refresh_at}");
        let mut parse = Vec::new();
        giggles_server::pgwire::z(&name, &mut parse);
        giggles_server::pgwire::z("ROLLBACK TRANSACTION", &mut parse);
        parse.extend_from_slice(&[0, 0]);
        let mut packet = Vec::new();
        giggles_server::pgwire::frame(b'P', &parse, &mut packet).unwrap();
        giggles_server::pgwire::frame(b'S', &[], &mut packet).unwrap();
        stream.write_all(&packet).await.unwrap();
        let parsed = wire_cycle(&mut stream).await;
        assert_eq!(tags(&parsed), b"1Z");
        assert_eq!(parsed.last().unwrap().1, b"I");
        if refresh_at == "describe" {
            ddl.execute(
                &format!("CREATE TABLE drift_{refresh_at} (id BIGINT PRIMARY KEY)"),
                &[],
                None,
            )
            .unwrap();
        }
        let mut describe = vec![b'S'];
        giggles_server::pgwire::z(&name, &mut describe);
        let mut packet = Vec::new();
        giggles_server::pgwire::frame(b'D', &describe, &mut packet).unwrap();
        giggles_server::pgwire::frame(b'S', &[], &mut packet).unwrap();
        stream.write_all(&packet).await.unwrap();
        let described = wire_cycle(&mut stream).await;
        assert_eq!(tags(&described), b"tnZ");
        assert_eq!(described.last().unwrap().1, b"I");
        if refresh_at == "bind" {
            ddl.execute(
                &format!("CREATE TABLE drift_{refresh_at} (id BIGINT PRIMARY KEY)"),
                &[],
                None,
            )
            .unwrap();
        }
        let mut bind = b"\0".to_vec();
        giggles_server::pgwire::z(&name, &mut bind);
        bind.extend_from_slice(&[0; 6]);
        let mut packet = Vec::new();
        for (tag, body) in [
            (b'B', bind.as_slice()),
            (b'D', &b"P\0"[..]),
            (b'E', &b"\0\0\0\0\0"[..]),
            (b'S', &b""[..]),
        ] {
            giggles_server::pgwire::frame(tag, body, &mut packet).unwrap();
        }
        stream.write_all(&packet).await.unwrap();
        let executed = wire_cycle(&mut stream).await;
        assert_eq!(tags(&executed), b"2nEZ", "{refresh_at}");
        assert!(executed.iter().any(|(tag, body)| {
            *tag == b'E'
                && body
                    .split(|byte| *byte == 0)
                    .any(|field| field == b"C25000")
        }));
        assert_eq!(executed.last().unwrap().1, b"I");
    }
    drop(stream);
    drop(ddl);
    agent.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
