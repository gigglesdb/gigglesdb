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
            "session-settings-{}-{}-{}",
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
        assert_eq!(connection.parameter("server_version"), Some("18.0"));
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
async fn postgres_18_version_settings_agree_across_protocol_surfaces() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    for (name, expected) in [("server_version", "18.0"), ("server_version_num", "180000")] {
        for sql in [
            format!("SHOW {name}"),
            format!("SELECT current_setting('{name}')"),
            format!("SELECT setting FROM pg_catalog.pg_settings WHERE name = '{name}'"),
        ] {
            assert_eq!(
                values(&client.simple_query(&sql).await.unwrap()),
                [expected]
            );
            let row = client.query_one(&sql, &[]).await.unwrap();
            assert_eq!(row.get::<_, String>(0), expected);
        }
        for sql in [
            format!("SET {name} = '1'"),
            format!("SELECT set_config('{name}', '1', false)"),
        ] {
            assert_eq!(code(client.simple_query(&sql).await.unwrap_err()), "22023");
        }
    }
    let version: String = client
        .query_one("SELECT version()", &[])
        .await
        .unwrap()
        .get(0);
    assert!(version.starts_with("PostgreSQL 18.0 (GigglesDB "));
    for (path, expected) in [("", "{}"), ("missing, public, public", r#"{"public"}"#)] {
        client
            .batch_execute(&format!("SET search_path = '{path}'"))
            .await
            .unwrap();
        assert_eq!(
            values(
                &client
                    .simple_query("SELECT current_schemas(false)")
                    .await
                    .unwrap()
            ),
            [expected]
        );
        let schemas: Vec<String> = client
            .query_one("SELECT current_schemas(false)", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            schemas,
            if path.is_empty() {
                vec![]
            } else {
                vec!["public"]
            }
        );
    }
    drop(client);
    fixture.stop().await;
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
async fn agent_settings_surface_is_unchanged() {
    let path = std::env::temp_dir().join(format!(
        "settings-agent-{}-{}",
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
    for sql in [
        "SET TIME ZONE 'UTC'",
        "SET DateStyle TO ISO",
        "SET search_path TO public",
        "SET transaction_timeout=0",
        "SET client_min_messages=warning",
        "SET row_security=off",
        "SET check_function_bodies=false",
        "SET xmloption=content",
        "SET default_tablespace=''",
        "SET default_table_access_method=heap",
        "SELECT pg_catalog.set_config('search_path', '', false)",
        "SELECT current_setting('transaction_timeout')",
        "SET LOCAL application_name='x'",
        "SET SESSION application_name='x'",
        "RESET application_name",
        "RESET ALL",
        "SHOW TIME ZONE",
        "SHOW IntervalStyle",
    ] {
        assert!(c.simple_query(sql).await.is_err(), "agent accepted {sql}");
        assert!(
            c.query(sql, &[]).await.is_err(),
            "agent extended accepted {sql}"
        );
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
async fn common_gucs_simple_and_extended_and_invalid_values() {
    let f = Fixture::new().await;
    let c = f.client().await;
    for (name, input, expected) in [
        ("application_name", "'a''b'", "a'b"),
        ("extra_float_digits", "-15", "-15"),
        ("TimeZone", "'America/Chicago'", "America/Chicago"),
        ("DateStyle", "'ISO, DMY'", "ISO, DMY"),
        ("client_encoding", "'UNICODE'", "UTF8"),
        ("standard_conforming_strings", "on", "on"),
        (
            "search_path",
            "'\"$user\", public, pg_catalog, \"Mixed Name\"'",
            "\"$user\", public, pg_catalog, \"Mixed Name\"",
        ),
        ("IntervalStyle", "postgres", "postgres"),
        ("bytea_output", "hex", "hex"),
        ("statement_timeout", "0", "0"),
        ("lock_timeout", "0", "0"),
        ("idle_in_transaction_session_timeout", "0", "0"),
    ] {
        c.simple_query(&format!("sEt SeSsIoN {name} tO {input}"))
            .await
            .unwrap();
        let shown = c.query(&format!("SHOW {name}"), &[]).await.unwrap();
        assert_eq!(shown[0].columns()[0].name(), name);
        assert_eq!(shown[0].get::<_, String>(0), expected);
        c.query(&format!("SET {name} = {input}"), &[])
            .await
            .unwrap();
        assert_eq!(
            values(&c.simple_query(&format!("SHOW {name}")).await.unwrap()),
            [expected]
        );
    }
    for input in ["UTF8", "'utf8'", "'UTF-8'", "'UNICODE'"] {
        c.query(&format!("SET client_encoding TO {input}"), &[])
            .await
            .unwrap();
    }
    for (input, expected) in [
        ("ISO", "ISO, MDY"),
        ("ISO, MDY", "ISO, MDY"),
        ("ISO, YMD", "ISO, YMD"),
    ] {
        c.query(&format!("SET DateStyle TO {input}"), &[])
            .await
            .unwrap();
        assert_eq!(
            values(&c.simple_query("SHOW DateStyle").await.unwrap()),
            [expected]
        );
    }
    for sql in [
        "SET extra_float_digits=4",
        "SET extra_float_digits=-16",
        "SET TimeZone='bad zone'",
        "SET DateStyle='SQL'",
        "SET client_encoding=LATIN1",
        "SET standard_conforming_strings=off",
        "SET IntervalStyle=iso_8601",
        "SET bytea_output=escape",
        "SET statement_timeout=1",
        "SET lock_timeout=1",
        "SET idle_in_transaction_session_timeout=1",
    ] {
        assert_eq!(
            code(c.simple_query(sql).await.unwrap_err()),
            "22023",
            "{sql}"
        );
        assert_eq!(code(c.query(sql, &[]).await.unwrap_err()), "22023", "{sql}");
    }
    for sql in [
        "SET application_name TO 1+2",
        "SET application_name TO $1",
        "SET TimeZone TO America/Chicago",
        "SET application_name TO foo-bar",
    ] {
        assert_eq!(
            code(c.simple_query(sql).await.unwrap_err()),
            "42601",
            "{sql}"
        );
        assert_eq!(code(c.query(sql, &[]).await.unwrap_err()), "42601", "{sql}");
    }
    let long = format!("SET application_name='{}'", "x".repeat(129));
    assert_eq!(code(c.simple_query(&long).await.unwrap_err()), "22023");
    for sql in ["SET nonexistent=1", "RESET nonexistent", "SHOW nonexistent"] {
        let e = c.simple_query(sql).await.unwrap_err();
        assert_eq!(
            e.as_db_error().unwrap().message(),
            "unrecognized configuration parameter"
        );
        assert_eq!(code(e), "42704");
        assert_eq!(code(c.query(sql, &[]).await.unwrap_err()), "42704");
    }
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn transaction_local_reset_and_multi_statement_settings() {
    let f = Fixture::new().await;
    let c = f.client().await;
    let default_path = r#""$user", public"#;
    // Initial SHOW and all reset forms use PostgreSQL's built-in default.
    assert_eq!(
        values(&c.simple_query("SHOW search_path").await.unwrap()),
        [default_path]
    );
    assert_eq!(
        c.query("SHOW search_path", &[]).await.unwrap()[0].get::<_, String>(0),
        default_path
    );
    for reset in [
        "RESET search_path",
        "RESET ALL",
        "SET search_path TO DEFAULT",
    ] {
        let sql = format!("SET search_path TO public; {reset}; SHOW search_path;");
        assert_eq!(
            values(&c.simple_query(&sql).await.unwrap()),
            [default_path],
            "{reset}"
        );
        c.query("SET search_path TO public", &[]).await.unwrap();
        c.query(reset, &[]).await.unwrap();
        assert_eq!(
            c.query("SHOW search_path", &[]).await.unwrap()[0].get::<_, String>(0),
            default_path,
            "{reset}"
        );
    }
    c.simple_query("SET application_name='session'; BEGIN; SET LOCAL application_name='local'; SHOW application_name; COMMIT").await.unwrap();
    assert_eq!(
        values(&c.simple_query("SHOW application_name").await.unwrap()),
        ["session"]
    );
    c.query("BEGIN", &[]).await.unwrap();
    c.query("SET SESSION application_name='kept'", &[])
        .await
        .unwrap();
    c.query("SET LOCAL application_name='local'", &[])
        .await
        .unwrap();
    assert_eq!(
        c.query("SHOW application_name", &[]).await.unwrap()[0].get::<_, String>(0),
        "local"
    );
    c.query("COMMIT", &[]).await.unwrap();
    assert_eq!(
        values(&c.simple_query("SHOW application_name").await.unwrap()),
        ["kept"]
    );
    c.simple_query("BEGIN; SET application_name='undo'; SET LOCAL TimeZone='GMT'; ROLLBACK")
        .await
        .unwrap();
    assert_eq!(
        values(
            &c.simple_query("SHOW application_name; SHOW TimeZone")
                .await
                .unwrap()
        ),
        ["kept", "UTC"]
    );
    c.simple_query(
        "BEGIN; SET LOCAL application_name='local'; SET application_name='later'; COMMIT",
    )
    .await
    .unwrap();
    assert_eq!(
        values(&c.simple_query("SHOW application_name").await.unwrap()),
        ["later"]
    );
    c.query("RESET application_name", &[]).await.unwrap();
    assert_eq!(
        values(&c.simple_query("SHOW application_name").await.unwrap()),
        [""]
    );
    assert_eq!(values(&c.simple_query("SET application_name='batch'; SHOW application_name; SET TIME ZONE 'GMT'; SHOW TIME ZONE").await.unwrap()), ["batch", "GMT"]);
    c.simple_query("BEGIN; RESET ALL; ROLLBACK").await.unwrap();
    assert_eq!(
        values(
            &c.simple_query("SHOW application_name; SHOW TIME ZONE")
                .await
                .unwrap()
        ),
        ["batch", "GMT"]
    );
    c.query("RESET ALL", &[]).await.unwrap();
    assert_eq!(values(&c.simple_query("SHOW application_name; SHOW TimeZone; SHOW DateStyle; SHOW search_path; SHOW extra_float_digits").await.unwrap()), ["", "UTC", "ISO", default_path, "1"]);
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn wire_status_warning_and_startup_defaults() {
    let f = Fixture::new().await;
    let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
    let body = b"\0\x03\0\0user\0owner\0database\0postgres\0TimeZone\0America/Chicago\0\0";
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
    wire_cycle(&mut stream).await;
    let set = wire_query(&mut stream, "SET TIME ZONE 'GMT'").await;
    assert_eq!(tags(&set), b"SCZ");
    assert_eq!(set[0].1, b"TimeZone\0GMT\0");
    let reset = wire_query(&mut stream, "SET TIME ZONE LOCAL").await;
    assert_eq!(reset[0].1, b"TimeZone\0America/Chicago\0");
    wire_query(&mut stream, "SET TIME ZONE 'GMT'").await;
    let reset = wire_query(&mut stream, "SET TimeZone TO LOCAL").await;
    assert_eq!(reset[0].1, b"TimeZone\0America/Chicago\0");
    let warning = wire_query(&mut stream, "SET LOCAL TimeZone='GMT'").await;
    assert_eq!(tags(&warning), b"NCZ");
    assert!(warning[0].1.split(|b| *b == 0).any(|v| v == b"C25P01"));
    assert!(
        warning[0]
            .1
            .split(|b| *b == 0)
            .any(|v| v == b"MSET LOCAL can only be used in transaction blocks")
    );
    wire_query(&mut stream, "BEGIN").await;
    wire_query(&mut stream, "SET LOCAL TimeZone='GMT'").await;
    let commit = wire_query(&mut stream, "COMMIT").await;
    assert_eq!(tags(&commit), b"SCZ");
    assert_eq!(commit[0].1, b"TimeZone\0America/Chicago\0");
    wire_query(&mut stream, "BEGIN").await;
    wire_query(&mut stream, "SET TimeZone='GMT'").await;
    let rollback = wire_query(&mut stream, "ROLLBACK").await;
    assert_eq!(tags(&rollback), b"SCZ");
    assert_eq!(rollback[0].1, b"TimeZone\0America/Chicago\0");
    // Extended SET LOCAL outside a block must warn too, before CommandComplete.
    let mut packet = Vec::new();
    for (tag, body) in [
        (b'P', &b"\0SET LOCAL TimeZone='GMT'\0\0\0"[..]),
        (b'B', &b"\0\0\0\0\0\0\0\0"[..]),
        (b'E', &b"\0\0\0\0\0"[..]),
        (b'S', &b""[..]),
    ] {
        giggles_server::pgwire::frame(tag, body, &mut packet).unwrap();
    }
    stream.write_all(&packet).await.unwrap();
    assert_eq!(tags(&wire_cycle(&mut stream).await), b"12NCZ");
    wire_query(&mut stream, "SET client_min_messages=error").await;
    assert_eq!(
        tags(&wire_query(&mut stream, "SET LOCAL TimeZone='GMT'").await),
        b"CZ"
    );
    wire_query(&mut stream, "RESET client_min_messages").await;
    // An extended implicit cycle is not an explicit transaction block.
    packet.clear();
    for (tag, body) in [
        (b'P', &b"\0SELECT 1 AS n\0\0\0"[..]),
        (b'B', &b"\0\0\0\0\0\0\0\0"[..]),
        (b'E', &b"\0\0\0\0\0"[..]),
        (b'P', &b"\0SET LOCAL TimeZone='GMT'\0\0\0"[..]),
        (b'B', &b"\0\0\0\0\0\0\0\0"[..]),
        (b'E', &b"\0\0\0\0\0"[..]),
        (b'S', &b""[..]),
    ] {
        giggles_server::pgwire::frame(tag, body, &mut packet).unwrap();
    }
    stream.write_all(&packet).await.unwrap();
    assert_eq!(tags(&wire_cycle(&mut stream).await), b"12DC12NCZ");
    let show = wire_query(&mut stream, "SHOW TimeZone").await;
    assert!(
        show.iter()
            .any(|(tag, body)| *tag == b'D' && body.ends_with(b"America/Chicago"))
    );
    drop(stream);
    f.stop().await;
}

#[tokio::test]
async fn settings_diagnostics_do_not_rewrite_core_parameter_errors() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE setting_diagnostics (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    c.simple_query("INSERT INTO setting_diagnostics VALUES (1)")
        .await
        .unwrap();
    let sql = "SELECT chr(-1) AS invalid_character FROM setting_diagnostics LIMIT 1";
    for extended in [false, true] {
        for (query, expected_code, expected_message) in [
            (sql, "22023", "request rejected; consult documented code"),
            (
                "SET lock_timeout=1",
                "22023",
                "invalid parameter value; not supported by this server profile",
            ),
            (sql, "22023", "request rejected; consult documented code"),
            (
                "SHOW nonexistent",
                "42704",
                "unrecognized configuration parameter",
            ),
            (
                "DROP ROLE missing_settings_diagnostic_role",
                "42704",
                "request rejected; consult documented code",
            ),
            (sql, "22023", "request rejected; consult documented code"),
        ] {
            let error = if extended {
                c.query(query, &[]).await.unwrap_err()
            } else {
                c.simple_query(query).await.unwrap_err()
            };
            assert_eq!(error.code().unwrap().code(), expected_code, "{query}");
            assert_eq!(
                error.as_db_error().unwrap().message(),
                expected_message,
                "{query}"
            );
        }
    }
    // The multi-statement error encoder must preserve the core diagnostic too.
    let error = c
        .simple_query(&format!("SET application_name='diagnostics'; {sql}"))
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "22023");
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "request rejected; consult documented code"
    );
    drop(c);
    f.stop().await;
}
