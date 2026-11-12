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
        ("IntervalStyle", "postgres_verbose", "postgres_verbose"),
        ("IntervalStyle", "sql_standard", "sql_standard"),
        ("IntervalStyle", "ISO_8601", "iso_8601"),
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
        "SET IntervalStyle=invalid",
        "SET bytea_output=escape",
        "SET statement_timeout=-1",
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

#[tokio::test]
async fn orm_interval_output_and_readonly_presets() {
    let f = Fixture::new().await;
    let c = f.client().await;
    for (style, expected) in [
        ("postgres", ["1 year 2 mons 3 days 04:05:06.7", "-1 years -2 mons -3 days -04:05:06.7", "00:00:00", "-1 years +2 days"]),
        ("postgres_verbose", ["@ 1 year 2 mons 3 days 4 hours 5 mins 6.7 secs", "@ 1 year 2 mons 3 days 4 hours 5 mins 6.7 secs ago", "@ 0", "@ 1 year -2 days ago"]),
        ("sql_standard", ["+1-2 +3 +4:05:06.7", "-1-2 -3 -4:05:06.7", "0", "-1-0 +2 +0:00:00"]),
        ("iso_8601", ["P1Y2M3DT4H5M6.7S", "P-1Y-2M-3DT-4H-5M-6.7S", "PT0S", "P-1Y2D"]),
    ] {
        c.batch_execute(&format!("SET IntervalStyle={style}")).await.unwrap();
        for (literal, expected) in ["1 year 2 mons 3 days 04:05:06.7", "-1 year -2 mons -3 days -04:05:06.7", "0", "-1 year 2 days"].into_iter().zip(expected) {
            let result = c.simple_query(&format!("SELECT interval '{literal}'")).await.unwrap();
            assert_eq!(values(&result), [expected], "{style}: {literal}");
        }
    }
    for (name, expected) in [("max_identifier_length", "63"), ("max_index_keys", "32"), ("block_size", "8192"), ("integer_datetimes", "on")] {
        assert_eq!(values(&c.simple_query(&format!("SHOW {name}")).await.unwrap()), [expected]);
    }
    for sql in ["SET max_identifier_length=63", "RESET block_size", "SELECT set_config('integer_datetimes', 'on', false)"] {
        assert_eq!(code(c.simple_query(sql).await.unwrap_err()), "55P02");
    }
    let sql = "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1), version(), current_setting('server_version_num')::integer as numeric_version";
    for (schema, expected) in [("public", true), ("missing", false)] {
        let row = c.query_one(sql, &[&schema]).await.unwrap();
        assert_eq!(row.get::<_, bool>(0), expected);
        assert_eq!(row.get::<_, i32>(2), 180000);
    }
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn advisory_locks_reentry_block_cancel_and_transaction_release() {
    use std::time::Duration;
    let fixture = Fixture::new().await;
    let a = fixture.client().await;
    let b = fixture.client().await;
    let boolean = |row: tokio_postgres::Row| row.get::<_, bool>(0);
    let lock = a.prepare("SELECT pg_advisory_lock(72707369)").await.unwrap();
    assert_eq!(lock.columns()[0].type_().oid(), 2278);
    a.query_one(&lock, &[]).await.unwrap();
    a.simple_query("SELECT pg_catalog.pg_advisory_lock(72707369)").await.unwrap();
    assert!(!boolean(b.query_one("SELECT pg_try_advisory_lock(72707369)", &[]).await.unwrap()));
    // The int,int key space is distinct from the bigint key space.
    assert!(boolean(b.query_one("SELECT pg_try_advisory_lock(0,72707369)", &[]).await.unwrap()));
    assert!(boolean(a.query_one("SELECT pg_advisory_unlock(72707369)", &[]).await.unwrap()));
    {
    let wait = b.simple_query("SELECT pg_advisory_lock(72707369)");
    tokio::pin!(wait);
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut wait).await.is_err());
    assert!(boolean(a.query_one("SELECT pg_advisory_unlock(72707369)", &[]).await.unwrap()));
    tokio::time::timeout(Duration::from_secs(2), &mut wait).await.unwrap().unwrap();
    }
    assert!(!boolean(a.query_one("SELECT pg_advisory_unlock(72707369)", &[]).await.unwrap()));
    a.simple_query("SET statement_timeout='30ms'").await.unwrap();
    assert_eq!(code(a.simple_query("SELECT pg_advisory_lock(72707369)").await.unwrap_err()), "57014");
    assert_eq!(values(&a.simple_query("SHOW statement_timeout").await.unwrap()), ["30ms"]);
    a.simple_query("BEGIN; SET LOCAL statement_timeout=0; COMMIT").await.unwrap();
    assert_eq!(values(&a.simple_query("SHOW statement_timeout").await.unwrap()), ["30ms"]);
    a.simple_query("RESET statement_timeout").await.unwrap();
    {
    let wait = a.simple_query("SELECT pg_advisory_lock(72707369)");
    tokio::pin!(wait);
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut wait).await.is_err());
    a.cancel_token().cancel_query(NoTls).await.unwrap();
    assert_eq!(code(wait.await.unwrap_err()), "57014");
    }
    b.simple_query("SELECT pg_advisory_unlock_all()").await.unwrap();
    for end in ["COMMIT", "ROLLBACK"] {
        a.simple_query("BEGIN; SELECT pg_advisory_xact_lock(-8,9); SELECT pg_advisory_unlock_all()").await.unwrap();
        assert!(!boolean(b.query_one("SELECT pg_try_advisory_xact_lock(-8,9)", &[]).await.unwrap()));
        assert!(!boolean(a.query_one("SELECT pg_advisory_unlock(-8,9)", &[]).await.unwrap()));
        a.simple_query(end).await.unwrap();
        assert!(boolean(b.query_one("SELECT pg_try_advisory_xact_lock(-8,9)", &[]).await.unwrap()));
    }
    a.simple_query("SELECT pg_advisory_lock(91)").await.unwrap();
    b.simple_query("SELECT pg_advisory_lock(92)").await.unwrap();
    {
    let wait = a.simple_query("SELECT pg_advisory_lock(92)");
    tokio::pin!(wait);
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut wait).await.is_err());
    assert_eq!(code(b.simple_query("SELECT pg_advisory_lock(91)").await.unwrap_err()), "40P01");
    b.simple_query("SELECT pg_advisory_unlock_all()").await.unwrap();
    wait.await.unwrap();
    }
    // FIFO waiters cannot be bypassed by a later request.
    let c = fixture.client().await;
    a.simple_query("SELECT pg_advisory_lock(93)").await.unwrap();
    {
        let first = b.simple_query("SELECT pg_advisory_lock(93)"); tokio::pin!(first);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut first).await.is_err());
        let second = c.simple_query("SELECT pg_advisory_lock(93)"); tokio::pin!(second);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut second).await.is_err());
        a.simple_query("SELECT pg_advisory_unlock(93)").await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut first).await.unwrap().unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut second).await.is_err());
        b.simple_query("SELECT pg_advisory_unlock(93)").await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut second).await.unwrap().unwrap();
    }
    drop(c);
    // Connection teardown releases every session acquisition.
    drop(a);
    tokio::time::timeout(Duration::from_secs(2), b.simple_query("SELECT pg_advisory_lock(91)")).await.unwrap().unwrap();
    drop(b);
    fixture.stop().await;
}

#[tokio::test]
async fn advisory_warning_and_disconnected_waiter_release_locks() {
    use std::time::Duration;
    let f = Fixture::new().await;
    let b = f.client().await;
    b.simple_query("SELECT pg_advisory_lock(101)").await.unwrap();
    let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
    let body = b"\0\x03\0\0user\0owner\0database\0postgres\0\0";
    stream.write_u32((body.len() + 4) as u32).await.unwrap(); stream.write_all(body).await.unwrap();
    wire_cycle(&mut stream).await;
    let messages = wire_query(&mut stream, "SELECT pg_advisory_unlock(101)").await;
    assert_eq!(tags(&messages), b"NTDCZ");
    assert!(messages[0].1.split(|b| *b == 0).any(|v| v == b"C01000"));
    wire_query(&mut stream, "SELECT pg_advisory_lock(102)").await;
    let mut body = Vec::new(); giggles_server::pgwire::z("SELECT pg_advisory_lock(101)", &mut body);
    let mut packet = Vec::new(); giggles_server::pgwire::frame(b'Q', &body, &mut packet).unwrap();
    stream.write_all(&packet).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(50), stream.read_u8()).await.is_err());
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !b.query_one("SELECT pg_try_advisory_lock(102)", &[]).await.unwrap().get::<_, bool>(0) { tokio::task::yield_now().await; }
    }).await.unwrap();
    drop(b); f.stop().await;
}
