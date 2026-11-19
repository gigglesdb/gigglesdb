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

#[tokio::test]
async fn catalog_introspection_mixed_calls_keep_core_errors() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE introspection_app (id integer PRIMARY KEY)").await.unwrap();
    for (sql, state) in [
        ("SELECT pg_get_indexdef(0,1,true), abs(-1)", "0A000"),
        ("SELECT * FROM (SELECT pg_catalog.pg_get_indexdef(0,1,true) AS col, lower('X') AS other) tmp", "0A000"),
        ("SELECT pg_get_indexdef(0,1,true) FROM introspection_app", "0A000"),
        ("SELECT public.pg_get_indexdef(0,1,true)", "42883"),
    ] {
        assert_eq!(code(c.simple_query(sql).await.unwrap_err()), state, "{sql}");
        assert_eq!(code(c.query(sql, &[]).await.unwrap_err()), state, "{sql}");
    }
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn catalog_introspection_table_free_and_derived_both_protocols() {
    let f = Fixture::new().await;
    let c = f.client().await;
    for sql in include_str!("../../../compat/gui/sample.sql").split(';').map(str::trim)
        .filter(|s| s.starts_with("CREATE SCHEMA") || s.starts_with("CREATE TABLE") || s.starts_with("CREATE INDEX")) {
        c.simple_query(sql).await.unwrap();
    }
    let oid: u32 = c.query_one("SELECT oid FROM pg_class WHERE relname='child_amount_idx'", &[]).await.unwrap().get(0);
    // PG 18.6 returns an empty string for an out-of-range index column, not NULL.
    for (expression, expected) in [
        (format!("pg_catalog.pg_get_indexdef({oid},1,true)"), Some("amount")),
        (format!("pg_get_indexdef({oid},2,true)"), Some("")),
        ("pg_get_indexdef(0,1,true)".into(), None),
        ("pg_get_indexdef(NULL,1,true)".into(), None),
        ("pg_get_constraintdef(0,true)".into(), None),
        ("pg_get_expr(NULL,0,true)".into(), None),
        ("format_type(23,NULL)".into(), Some("integer")),
        ("obj_description(0,'pg_class')".into(), None),
        ("col_description(0,1)".into(), None),
        ("shobj_description(0,'pg_database')".into(), None),
    ] {
        for sql in [
            format!("SELECT {expression} AS column"),
            format!("SELECT * FROM (SELECT {expression} AS column, 0 AS dummy) tmp ORDER BY dummy"),
        ] {
            let messages = c.simple_query(&sql).await.unwrap();
            let rows: Vec<_> = messages.iter().filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r), _ => None,
            }).collect();
            assert_eq!(rows.len(), 1, "{sql}");
            assert_eq!(rows[0].get(0), expected, "{sql}");
            let row = c.query_one(&sql, &[]).await.unwrap();
            assert_eq!(row.get::<_, Option<String>>(0).as_deref(), expected, "{sql}");
            if row.len() == 2 {
                assert_eq!(row.get::<_, i32>(1), 0);
            }
        }
    }
    let stmt = c.prepare("SELECT pg_catalog.pg_get_indexdef($1, $2, $3)").await.unwrap();
    assert_eq!(c.query_one(&stmt, &[&oid, &1i32, &true]).await.unwrap().get::<_, String>(0), "amount");
    let user: String = c.query_one("SELECT pg_get_userbyid(10)", &[]).await.unwrap().get(0);
    let catalog_user: String = c.query_one("SELECT pg_get_userbyid(10) FROM pg_namespace LIMIT 1", &[]).await.unwrap().get(0);
    assert_eq!(user, catalog_user);
    c.simple_query("CREATE ROLE introspection_reader LOGIN").await.unwrap();
    let (reader, connection) = Config::new().hostaddr(f.pg.address.ip()).port(f.pg.address.port())
        .user("introspection_reader").dbname("postgres").connect(NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    let sql = format!("SELECT pg_get_indexdef({oid},1,true)");
    let prepared = reader.prepare(&sql).await.unwrap();
    // Catalog visibility and fresh authorization apply even to an already
    // prepared table-free call, including after a privilege is revoked.
    for (grant, expected) in [(false, None), (true, Some("amount")), (false, None)] {
        let action = if grant { "GRANT SELECT ON gui_sales.child TO" } else { "REVOKE SELECT ON gui_sales.child FROM" };
        c.simple_query(&format!("{action} introspection_reader")).await.unwrap();
        let messages = reader.simple_query(&sql).await.unwrap();
        let row = messages.iter().find_map(|m| match m { SimpleQueryMessage::Row(r) => Some(r), _ => None }).unwrap();
        assert_eq!(row.get(0), expected);
        assert_eq!(reader.query_one(&prepared, &[]).await.unwrap().get::<_, Option<String>>(0).as_deref(), expected);
    }
    drop(reader);
    connection.await.unwrap().unwrap();
    drop(c);
    f.stop().await;
}

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
        assert_eq!(connection.parameter("server_version"), Some("18.6"));
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
async fn build_identity_and_capabilities_agree_across_protocol_surfaces() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    let capabilities = giggles_pg_catalog::CAPABILITIES;
    for sql in [
        "SHOW giggles_capabilities",
        "SHOW GiGgLeS_CaPaBiLiTiEs",
        "SELECT current_setting('giggles_capabilities')",
        "SELECT pg_catalog.current_setting('GiGgLeS_CaPaBiLiTiEs')",
        "SELECT current_setting('giggles_capabilities', true)",
        "SELECT pg_catalog.current_setting('giggles_capabilities', false)",
        "SELECT current_setting('giggles_capabilities') FROM pg_catalog.pg_roles LIMIT 1",
        "SELECT pg_catalog.current_setting('GiGgLeS_CaPaBiLiTiEs', true) FROM pg_catalog.pg_roles LIMIT 1",
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'giggles_capabilities'",
    ] {
        assert_eq!(values(&client.simple_query(sql).await.unwrap()), [capabilities], "{sql}");
        assert_eq!(client.query_one(sql, &[]).await.unwrap().get::<_, String>(0), capabilities, "{sql}");
    }
    for (version_sql, capabilities_sql) in [
        ("SET server_version = '1'", "SET giggles_capabilities = 'changed'"),
        ("RESET server_version", "RESET giggles_capabilities"),
        ("SELECT set_config('server_version', '1', false)", "SELECT set_config('giggles_capabilities', 'changed', false)"),
        ("SELECT set_config('server_version', '1', true)", "SELECT pg_catalog.set_config('GiGgLeS_CaPaBiLiTiEs', 'changed', true)"),
    ] {
        let expected = code(client.simple_query(version_sql).await.unwrap_err());
        assert_eq!(expected, "22023");
        assert_eq!(code(client.simple_query(capabilities_sql).await.unwrap_err()), expected, "{capabilities_sql}");
        let expected = code(client.query(version_sql, &[]).await.unwrap_err());
        assert_eq!(code(client.query(capabilities_sql, &[]).await.unwrap_err()), expected, "{capabilities_sql}");
    }
    let build_id = giggles_pg_catalog::BUILD_ID;
    assert!(build_id == "unknown" || (build_id.len() == 12
        && build_id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))));
    let expected = format!("PostgreSQL 18.6 (GigglesDB 0.1.0+{build_id})");
    for sql in ["SELECT version()", "SELECT pg_catalog.version() FROM pg_catalog.pg_roles LIMIT 1"] {
        assert_eq!(values(&client.simple_query(sql).await.unwrap()), [expected.as_str()], "{sql}");
        assert_eq!(client.query_one(sql, &[]).await.unwrap().get::<_, String>(0), expected, "{sql}");
    }
    println!("observed version(): {expected}");
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn server_encoding_is_utf8_and_read_only_across_protocol_surfaces() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    for sql in [
        "SHOW server_encoding",
        "SHOW SeRvEr_EnCoDiNg",
        "SELECT current_setting('server_encoding')",
        "SELECT pg_catalog.current_setting('server_encoding')",
        "SELECT current_setting('server_encoding', true)",
        "SELECT pg_catalog.current_setting('SeRvEr_EnCoDiNg', true)",
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'server_encoding'",
        "SELECT current_setting('server_encoding') FROM pg_catalog.pg_roles LIMIT 1",
        "SELECT pg_catalog.current_setting('SeRvEr_EnCoDiNg') FROM pg_catalog.pg_roles LIMIT 1",
    ] {
        assert_eq!(values(&client.simple_query(sql).await.unwrap()), ["UTF8"], "{sql}");
        assert_eq!(client.query_one(sql, &[]).await.unwrap().get::<_, String>(0), "UTF8", "{sql}");
    }
    for (version_sql, encoding_sql) in [
        ("SET server_version = '1'", "SET server_encoding = 'LATIN1'"),
        ("SET server_version = '1'", "SET SeRvEr_EnCoDiNg = 'UTF8'"),
        ("SELECT set_config('server_version', '1', false)", "SELECT set_config('server_encoding', 'LATIN1', false)"),
        ("SELECT set_config('server_version', '1', true)", "SELECT pg_catalog.set_config('SeRvEr_EnCoDiNg', 'UTF8', true)"),
    ] {
        let expected = code(client.simple_query(version_sql).await.unwrap_err());
        assert_eq!(expected, "22023");
        assert_eq!(code(client.simple_query(encoding_sql).await.unwrap_err()), expected, "{encoding_sql}");
        let expected = code(client.query(version_sql, &[]).await.unwrap_err());
        assert_eq!(expected, "22023");
        assert_eq!(code(client.query(encoding_sql, &[]).await.unwrap_err()), expected, "{encoding_sql}");
    }
    assert_eq!(values(&client.simple_query("SHOW server_encoding").await.unwrap()), ["UTF8"]);
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn postgres_18_version_settings_agree_across_protocol_surfaces() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    for (name, expected) in [("server_version", "18.6"), ("server_version_num", "180006")] {
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
    assert!(version.starts_with("PostgreSQL 18.6 (GigglesDB "));
    for (path, expected) in [("", "{}"), ("missing, public, public", r#"{public}"#)] {
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
    for (input, expected) in [("1", "1ms"), ("'2s'", "2s"), ("0", "0")] {
        let sql = format!("SET lock_timeout={input}");
        c.simple_query(&sql).await.unwrap();
        assert_eq!(values(&c.simple_query("SHOW lock_timeout").await.unwrap()), [expected]);
        c.query(&sql, &[]).await.unwrap();
        let rows = c.query("SHOW lock_timeout", &[]).await.unwrap();
        assert_eq!(rows[0].get::<_, String>(0), expected);
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
        "SET lock_timeout=-1",
        "SET lock_timeout='abc'",
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
    assert_eq!(values(&c.simple_query("SHOW application_name; SHOW TimeZone; SHOW DateStyle; SHOW search_path; SHOW extra_float_digits").await.unwrap()), ["", "UTC", "ISO, MDY", default_path, "1"]);
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
                "SET idle_in_transaction_session_timeout=1",
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
        assert_eq!(row.get::<_, i32>(2), 180006);
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

#[tokio::test]
async fn pgadmin_numeric_settings_inventory_shapes() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    let sql = "SELECT name,setting::numeric AS setting,min_val,max_val,unit,vartype,context,source,boot_val,reset_val,pending_restart FROM pg_catalog.pg_show_all_settings() WHERE name IN ('autovacuum_vacuum_cost_delay','autovacuum_vacuum_threshold','missing') ORDER BY name";
    let messages = client.simple_query(sql).await.unwrap();
    let rows: Vec<_> = messages.iter().filter_map(|m| if let SimpleQueryMessage::Row(r) = m { Some((0..r.len()).map(|i| r.get(i)).collect::<Vec<_>>()) } else { None }).collect();
    assert_eq!(rows, vec![vec![Some("autovacuum_vacuum_cost_delay"),Some("2"),Some("-1"),Some("100"),Some("ms"),Some("real"),Some("sighup"),Some("default"),Some("2"),Some("2"),Some("f")],vec![Some("autovacuum_vacuum_threshold"),Some("50"),Some("0"),Some("2147483647"),None,Some("integer"),Some("sighup"),Some("default"),Some("50"),Some("50"),Some("f")]]);
    let statement = client.prepare(sql).await.unwrap();
    assert_eq!(statement.columns().iter().map(|c| c.type_().oid()).collect::<Vec<_>>(), vec![25,1700,25,25,25,25,25,25,25,25,16]);
    assert_eq!(client.query(&statement, &[]).await.unwrap().len(), 2);
    assert!(client.simple_query("SELECT name,setting::numeric AS setting FROM pg_show_all_settings() WHERE name IN ('AUTOVACUUM_VACUUM_THRESHOLD') ORDER BY name").await.unwrap().iter().all(|m| !matches!(m, SimpleQueryMessage::Row(_))));
    drop(client);
    fixture.stop().await;
}


#[tokio::test]
async fn pgparity_timezone_forms_drive_output_and_conversions() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.batch_execute("CREATE TABLE zone_probe (id int); INSERT INTO zone_probe VALUES (1)").await.unwrap();
    for (input, label, output) in [
        ("'America/New_York'", "America/New_York", "2024-01-01 22:04:05-05"),
        ("'+05:30'", "+05:30", "2024-01-01 21:34:05-05:30"),
        ("'05:30'", "05:30", "2024-01-01 21:34:05-05:30"),
        ("'UTC+3'", "UTC+3", "2024-01-02 00:04:05-03"),
        ("'UTC-05:30'", "UTC-05:30", "2024-01-02 08:34:05+05:30"),
        ("'UTC+99'", "UTC+99", "2023-12-29 00:04:05-99"),
        ("'UTC+15'", "UTC+15", "2024-01-01 12:04:05-15"),
        ("'PST8PDT'", "PST8PDT", "2024-01-01 19:04:05-08"),
        ("'-03'", "<-03>+03", "2024-01-02 00:04:05-03"),
        ("'+5'", "<+05>-05", "2024-01-02 08:04:05+05"),
        ("2", "<+02>-02", "2024-01-02 05:04:05+02"),
        ("-2.5", "<-02:30>+02:30", "2024-01-02 00:34:05-02:30"),
        ("2.25", "<+02:15>-02:15", "2024-01-02 05:19:05+02:15"),
        ("15", "<+15>-15", "2024-01-02 18:04:05+15"),
        ("16", "<+16>-16", "2024-01-02 19:04:05+16"),
        ("-15", "<-15>+15", "2024-01-01 12:04:05-15"),
        ("INTERVAL '+05:30' HOUR TO MINUTE", "<+05:30>-05:30", "2024-01-02 08:34:05+05:30"),
        ("INTERVAL '-08:00'", "<-08>+08", "2024-01-01 19:04:05-08"),
        ("'utc'", "UTC", "2024-01-02 03:04:05+00"),
        ("'europe/paris'", "Europe/Paris", "2024-01-02 04:04:05+01"),
        ("'GMT'", "GMT", "2024-01-02 03:04:05+00"),
        ("'Etc/GMT+3'", "Etc/GMT+3", "2024-01-02 00:04:05-03"),
    ] {
        c.batch_execute(&format!("SET TIME ZONE {input}")).await.unwrap();
        assert_eq!(values(&c.simple_query("SHOW TimeZone").await.unwrap()), [label], "{input}");
        for suffix in ["", " FROM zone_probe"] {
            let sql = format!("SELECT TIMESTAMPTZ '2024-01-02 03:04:05+00'{suffix}");
            assert_eq!(values(&c.simple_query(&sql).await.unwrap()), [output], "{input} {sql}");
            let sql = format!("SELECT (TIMESTAMPTZ '2024-01-02 03:04:05+00')::timestamp{suffix}");
            assert_eq!(values(&c.simple_query(&sql).await.unwrap()), [&output[..19]], "{input} cast");
            let sql = format!("SELECT timezone('{label}', TIMESTAMPTZ '2024-01-02 03:04:05+00'){suffix}");
            assert_eq!(values(&c.simple_query(&sql).await.unwrap()), [&output[..19]], "{input} timezone()");
            let sql = format!("SELECT (TIMESTAMPTZ '2024-01-02 03:04:05+00')::date{suffix}");
            assert_eq!(values(&c.simple_query(&sql).await.unwrap()), [&output[..10]], "{input} date cast");
            let sql = format!("SELECT now()::timestamp = localtimestamp{suffix}");
            assert_eq!(values(&c.simple_query(&sql).await.unwrap()), ["t"], "{input} now()");
            let sql = format!("SELECT (TIMESTAMP '{}')::timestamptz{suffix}", &output[..19]);
            assert_eq!(values(&c.simple_query(&sql).await.unwrap()), [output], "{input} inverse cast");
        }
    }
    for (zone, output) in [("XYZ-3:30", "2024-07-02 06:34:05+03:30"), ("EST5EDT", "2024-07-01 23:04:05-04")] {
        c.batch_execute(&format!("SET TIME ZONE '{zone}'")).await.unwrap();
        assert_eq!(values(&c.simple_query("SELECT TIMESTAMPTZ '2024-07-02 03:04:05+00'").await.unwrap()), [output]);
    }
    for sql in ["SET TIME ZONE 'Not/AZone'", "SET TIME ZONE 'Z'", "SET TIME ZONE 'zulu'", "SET TIME ZONE 'PST'", "SET TIME ZONE 'abc'", "SET TIME ZONE ''", "SET TIME ZONE INTERVAL '1 day'"] {
        let e = c.simple_query(sql).await.unwrap_err();
        let db = e.as_db_error().unwrap();
        assert_eq!(db.message(), "invalid value for parameter \"TimeZone\"", "{sql}");
        if sql.contains("INTERVAL") {
            assert_eq!(db.detail(), Some("Cannot specify days in time zone interval"));
        }
        assert_eq!(code(e), "22023", "{sql}");
        assert_eq!(values(&c.simple_query("SHOW TimeZone").await.unwrap()), ["EST5EDT"]);
    }
    assert_eq!(code(c.simple_query("SET TIME ZONE INTERVAL '+05:30:15' HOUR TO SECOND").await.unwrap_err()), "42601");
    for sql in ["SET TIME ZONE LOCAL", "SET TIME ZONE DEFAULT", "RESET TimeZone"] {
        c.batch_execute(sql).await.unwrap();
        assert_eq!(values(&c.simple_query("SHOW TimeZone").await.unwrap()), ["UTC"]);
    }
    c.batch_execute("SET TimeZone = '+05:30'").await.unwrap();
    assert_eq!(values(&c.simple_query("SHOW TimeZone").await.unwrap()), ["+05:30"]);
    c.batch_execute("SET TimeZone TO 'utc'").await.unwrap();
    drop(c); f.stop().await;
}

#[tokio::test]
async fn pgparity_version_datestyle_schemas_and_tags() {
    fn all_values(messages: &[SimpleQueryMessage]) -> Vec<String> {
        messages.iter().flat_map(|m| match m {
            SimpleQueryMessage::Row(row) => (0..row.len()).map(|i| row.get(i).unwrap().to_owned()).collect(),
            _ => Vec::new(),
        }).collect()
    }
    let f = Fixture::new().await;
    let c = f.client().await;
    for (sql, expected) in [
        ("SHOW server_version_num", vec!["180006"]),
        ("SELECT current_setting('server_version_num')", vec!["180006"]),
        ("SHOW server_version", vec!["18.6"]),
        ("SELECT split_part(version(), ' ', 1), split_part(version(), ' ', 2)", vec!["PostgreSQL", "18.6"]),
        ("SHOW DateStyle", vec!["ISO, MDY"]),
        ("SELECT current_setting('DateStyle')", vec!["ISO, MDY"]),
        ("SELECT current_schemas(false), current_schemas(true)", vec!["{public}", "{pg_catalog,public}"]),
    ] {
        assert_eq!(all_values(&c.simple_query(sql).await.unwrap()), expected, "{sql}");
    }
    c.batch_execute("SET DateStyle='ISO'; CREATE SCHEMA r0c_s; SET search_path TO r0c_s, public").await.unwrap();
    assert_eq!(all_values(&c.simple_query("SHOW DateStyle; SELECT current_schemas(false), current_schemas(true)").await.unwrap()), ["ISO, MDY", "{r0c_s,public}", "{pg_catalog,r0c_s,public}"]);
    c.batch_execute("SET DateStyle='ISO, DMY'").await.unwrap();
    assert_eq!(all_values(&c.simple_query("SHOW DateStyle; SELECT current_setting('DateStyle')").await.unwrap()), ["ISO, DMY", "ISO, DMY"]);
    let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
    let startup = b"\0\x03\0\0user\0owner\0database\0postgres\0\0";
    stream.write_u32((startup.len() + 4) as u32).await.unwrap();
    stream.write_all(startup).await.unwrap();
    let statuses = wire_cycle(&mut stream).await;
    assert!(statuses.iter().any(|(tag, b)| *tag == b'S' && b == b"server_version\x0018.6\0"));
    assert!(statuses.iter().any(|(tag, b)| *tag == b'S' && b == b"DateStyle\0ISO, MDY\0"));
    for (begin, end) in [("START TRANSACTION", "END"), ("START TRANSACTION READ ONLY", "END WORK"), ("BEGIN", "COMMIT WORK"), ("BEGIN WORK", "ABORT"), ("BEGIN TRANSACTION", "ABORT WORK"), ("BEGIN", "ROLLBACK WORK")] {
        for (sql, expected) in [(begin, if begin.starts_with("START") { "START TRANSACTION" } else { "BEGIN" }), (end, if end.starts_with("ABORT") || end.starts_with("ROLLBACK") { "ROLLBACK" } else { "COMMIT" })] {
            let messages = wire_query(&mut stream, sql).await;
            assert!(messages.iter().any(|(tag, body)| *tag == b'C' && body == format!("{expected}\0").as_bytes()), "{sql} {messages:?}");
        }
    }
    drop(c); f.stop().await;
}
