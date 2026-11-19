use giggles_core::{
    Admin, Limits,
    roles::{NewRole, RoleChanges},
};
use giggles_server::{
    Binding, Role, Running, Service,
    auth::{PostgresListener, PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
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
use tokio_postgres::{Client, Config, NoTls, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;
static SERIAL: AtomicU64 = AtomicU64::new(0);
pub(crate) const PASSWORD: &str = "s3cret-pass";
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
pub(crate) struct Fixture {
    admin: Admin,
    service: Arc<Service>,
    pub(crate) pg: Running,
    ca: Cert,
    app: Cert,
    server: Cert,
}
impl Fixture {
    pub(crate) async fn new(mode: TlsMode, trust: bool, mapped: bool) -> Self {
        let path = std::env::temp_dir().join(format!(
            "postgresql-network-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let ca = cert(None, false);
        let server = cert(Some(&ca), true);
        let app = cert(Some(&ca), false);
        for (name, c) in [("ca", &ca), ("server", &server), ("app", &app)] {
            std::fs::write(path.join(format!("{name}.pem")), c.cert.pem()).unwrap();
            std::fs::write(
                path.join(format!("{name}.key.TEST-ONLY")),
                c.key.serialize_pem(),
            )
            .unwrap();
        }
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        admin
            .sql_session()
            .execute(
                "CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT)",
                &[],
                None,
            )
            .unwrap();
        for (name, login) in [("owner", true), ("other", true), ("nologin", false)] {
            admin
                .roles()
                .create(NewRole {
                    name: name.into(),
                    superuser: true,
                    login,
                    password: Some(Verifier::generate(PASSWORD)),
                    ..NewRole::default()
                })
                .unwrap();
        }
        let bindings = vec![Binding {
            fingerprint: tls::fingerprint(app.cert.der()),
            role: Role::Application {
                domain: admin.authority_metadata().unwrap().0.to_string(),
                tables: BTreeSet::from(["app".into()]),
            },
        }];
        let service = Service::new(admin.clone(), bindings, Clock::new(0, 3600).unwrap()).unwrap();
        let config = PostgresqlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: mode,
            certificate: (mode != TlsMode::Disabled)
                .then(|| path.join("server.pem").to_string_lossy().into_owned()),
            private_key: (mode != TlsMode::Disabled).then(|| {
                path.join("server.key.TEST-ONLY")
                    .to_string_lossy()
                    .into_owned()
            }),
            client_ca: (mode != TlsMode::Disabled)
                .then(|| path.join("ca.pem").to_string_lossy().into_owned()),
            certificates: if mapped {
                BTreeMap::from([(tls::fingerprint(app.cert.der()), "owner".into())])
            } else {
                BTreeMap::new()
            },
            databases: vec!["postgres".into()],
            loopback_trust: trust,
            connections: 32,
        };
        let listener: PostgresListener = config.build().unwrap();
        let pg = serve_postgresql(config.listen, service.clone(), Arc::new(listener))
            .await
            .unwrap();
        Self {
            admin,
            service,
            pg,
            ca,
            app,
            server,
        }
    }
    pub(crate) fn config(&self, user: &str, password: Option<&str>, tls: bool) -> Config {
        let mut c = Config::new();
        c.host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
            .dbname("postgres")
            .ssl_mode(if tls {
                SslMode::Require
            } else {
                SslMode::Disable
            });
        if let Some(p) = password {
            c.password(p);
        }
        c
    }
    fn tls(&self, cert: bool) -> MakeRustlsConnect {
        let roots = tls::roots(vec![self.ca.cert.der().clone()]).unwrap();
        let builder = ClientConfig::builder().with_root_certificates(roots);
        let c = if cert {
            builder
                .with_client_auth_cert(
                    vec![self.app.cert.der().clone()],
                    PrivatePkcs8KeyDer::from(self.app.key.serialize_der()).into(),
                )
                .unwrap()
        } else {
            builder.with_no_client_auth()
        };
        MakeRustlsConnect::new(c)
    }
    pub(crate) async fn plain(&self, user: &str, password: Option<&str>) -> Client {
        let (c, connection) = self
            .config(user, password, false)
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        c
    }
    async fn secure(&self, user: &str, password: Option<&str>, cert: bool) -> Client {
        let (c, connection) = self
            .config(user, password, true)
            .connect(self.tls(cert))
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        c
    }
    pub(crate) async fn stop(self) {
        self.pg.shutdown().await.unwrap();
    }
}
fn code(error: tokio_postgres::Error) -> String {
    error.code().expect("server SQLSTATE").code().into()
}
async fn transaction(c: &Client) {
    c.batch_execute("BEGIN").await.unwrap();
    c.batch_execute("ROLLBACK").await.unwrap();
}
#[tokio::test]
async fn owner_password_crud_ddl_and_catalog() {
    let f = Fixture::new(TlsMode::Disabled, false, false).await;
    let c = f.plain("owner", Some(PASSWORD)).await;
    c.batch_execute("CREATE TABLE items (id BIGINT PRIMARY KEY, body TEXT)")
        .await
        .unwrap();
    assert_eq!(
        c.execute("INSERT INTO items VALUES ($1, $2)", &[&1i64, &"one"])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        c.execute("INSERT INTO items VALUES (2, 'two')", &[])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        c.query_one("SELECT body FROM items WHERE id = $1", &[&1i64])
            .await
            .unwrap()
            .get::<_, String>(0),
        "one"
    );
    assert_eq!(
        c.execute("UPDATE items SET body = 'after' WHERE id = 1", &[])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        c.query_one("SELECT body FROM items WHERE id = 1", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "after"
    );
    assert_eq!(
        c.execute("DELETE FROM items WHERE id = 2", &[])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        c.query_one("SELECT count(*) FROM items LIMIT 1", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    let later = f.plain("owner", Some(PASSWORD)).await;
    // HEAD network.rs catalog_multiple_granted_tables_composite_key_order_and_empty_visible_set,
    // with an explicit bound added to its pg_class query.
    let rows=later.query("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace=2200 AND relkind='r' ORDER BY relname LIMIT 100", &[]).await.unwrap();
    assert!(rows.iter().any(|r| r.get::<_, String>(0) == "items"));
    c.batch_execute("BEGIN").await.unwrap();
    c.batch_execute("CREATE TABLE t2 (id BIGINT PRIMARY KEY, body TEXT)")
        .await
        .unwrap();
    c.execute("INSERT INTO t2 VALUES (7, 'staged')", &[])
        .await
        .unwrap();
    c.batch_execute("COMMIT").await.unwrap();
    let rows = later.query("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace=2200 AND relkind='r' ORDER BY relname LIMIT 100", &[]).await.unwrap();
    assert!(rows.iter().any(|r| r.get::<_, String>(0) == "t2"));
    assert_eq!(
        later
            .query_one("SELECT body FROM t2 WHERE id = 7", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "staged"
    );
    drop(c);
    drop(later);
    f.stop().await;
}
#[tokio::test]
async fn wrong_password_unknown_role_nologin_and_unknown_database() {
    let f = Fixture::new(TlsMode::Disabled, false, false).await;
    for (user, password) in [
        ("owner", "wrong"),
        ("missing", PASSWORD),
        ("nologin", PASSWORD),
    ] {
        assert_eq!(
            code(
                f.config(user, Some(password), false)
                    .connect(NoTls)
                    .await
                    .err()
                    .unwrap()
            ),
            "28P01"
        );
    }
    let mut c = f.config("owner", Some(PASSWORD), false);
    c.dbname("missing");
    assert_eq!(code(c.connect(NoTls).await.err().unwrap()), "3D000");
    f.stop().await;
}
#[tokio::test]
async fn tls_required_refuses_plaintext_and_accepts_tls_password() {
    let f = Fixture::new(TlsMode::Required, false, false).await;
    assert!(
        f.config("owner", Some(PASSWORD), false)
            .connect(NoTls)
            .await
            .is_err()
    );
    let c = f.secure("owner", Some(PASSWORD), false).await;
    transaction(&c).await;
    drop(c);
    f.stop().await;
}
#[tokio::test]
async fn tls_optional_accepts_both() {
    let f = Fixture::new(TlsMode::Optional, false, false).await;
    let plain = f.plain("owner", Some(PASSWORD)).await;
    transaction(&plain).await;
    let secure = f.secure("owner", Some(PASSWORD), false).await;
    transaction(&secure).await;
    drop(plain);
    drop(secure);
    f.stop().await;
}
#[tokio::test]
async fn certificate_maps_to_role_without_password() {
    let f = Fixture::new(TlsMode::Optional, false, true).await;
    let c = f.secure("owner", None, true).await;
    transaction(&c).await;
    assert!(
        f.config("other", None, true)
            .connect(f.tls(true))
            .await
            .is_err()
    );
    drop(c);
    f.stop().await;
}
// Drivers stop locally on an absent password. Send an empty SASL response to
// exercise the server's rejection and retain an actual wire SQLSTATE assertion.
async fn missing_password_code(address: std::net::SocketAddr, user: &str) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut body = 196608u32.to_be_bytes().to_vec();
    body.extend_from_slice(format!("user\0{user}\0database\0postgres\0\0").as_bytes());
    stream
        .write_all(&((body.len() + 4) as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&body).await.unwrap();
    loop {
        let tag = stream.read_u8().await.unwrap();
        let length = stream.read_u32().await.unwrap();
        assert!((4..=65536).contains(&length));
        let mut body = vec![0; length as usize - 4];
        stream.read_exact(&mut body).await.unwrap();
        if tag == b'E' {
            return body
                .split(|b| *b == 0)
                .find_map(|field| {
                    field
                        .strip_prefix(b"C")
                        .map(|s| String::from_utf8(s.to_vec()).unwrap())
                })
                .expect("SQLSTATE");
        }
        if tag == b'R' {
            let mut response = b"SCRAM-SHA-256\0".to_vec();
            response.extend_from_slice(&0u32.to_be_bytes());
            stream.write_u8(b'p').await.unwrap();
            stream.write_u32((response.len() + 4) as u32).await.unwrap();
            stream.write_all(&response).await.unwrap();
        }
    }
}
#[tokio::test]
async fn loopback_trust_only_when_enabled() {
    let trusted = Fixture::new(TlsMode::Disabled, true, false).await;
    let c = trusted.plain("owner", None).await;
    transaction(&c).await;
    drop(c);
    trusted.stop().await;
    let strict = Fixture::new(TlsMode::Disabled, false, false).await;
    assert_eq!(
        missing_password_code(strict.pg.address, "owner").await,
        "28P01"
    );
    strict.stop().await;
}
#[tokio::test]
async fn agent_identity_gains_no_role_powers() {
    let f = Fixture::new(TlsMode::Optional, false, false).await;
    assert!(
        f.config("giggles", None, true)
            .connect(f.tls(true))
            .await
            .is_err()
    );
    let tls = tls::server(
        vec![f.server.cert.der().clone()],
        PrivatePkcs8KeyDer::from(f.server.key.serialize_der()).into(),
        vec![f.ca.cert.der().clone()],
    )
    .unwrap();
    let agent = serve_pg("127.0.0.1:0".parse().unwrap(), f.service.clone(), tls, 32)
        .await
        .unwrap();
    let mut config = Config::new();
    config
        .host("localhost")
        .hostaddr(agent.address.ip())
        .port(agent.address.port())
        .user("giggles")
        .dbname("giggles")
        .ssl_mode(SslMode::Require);
    let (c, conn) = config.connect(f.tls(true)).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    assert_eq!(
        code(
            c.batch_execute("CREATE TABLE forbidden (id BIGINT PRIMARY KEY)")
                .await
                .unwrap_err()
        ),
        "42501"
    );
    drop(c);
    agent.shutdown().await.unwrap();
    f.stop().await;
}
#[cfg(feature = "legacy-task")]
#[tokio::test]
async fn postgresql_session_cannot_run_task_statements() {
    let f = Fixture::new(TlsMode::Disabled, false, false).await;
    let c = f.plain("owner", Some(PASSWORD)).await;
    for sql in ["GIGGLES {\"kind\":\"Ping\"}", "GIGGLES STATUS ($1)"] {
        assert_eq!(code(c.simple_query(sql).await.unwrap_err()), "42501");
    }
    drop(c);
    f.stop().await;
}
#[tokio::test]
async fn role_change_ends_session() {
    let f = Fixture::new(TlsMode::Disabled, false, false).await;
    let c = f.plain("owner", Some(PASSWORD)).await;
    transaction(&c).await;
    f.admin
        .roles()
        .alter(
            "owner",
            RoleChanges {
                password: Some(Some(Verifier::generate("changed-password"))),
                ..RoleChanges::default()
            },
        )
        .unwrap();
    assert!(c.batch_execute("BEGIN").await.is_err());
    drop(c);
    f.stop().await;
}
#[tokio::test]
async fn cancel_request_reaches_only_postgresql_backends() {
    let f = Fixture::new(TlsMode::Disabled, false, false).await;
    let c = f.plain("owner", Some(PASSWORD)).await;
    c.cancel_token().cancel_query(NoTls).await.unwrap();
    transaction(&c).await;
    drop(c);
    f.stop().await;
}

async fn negotiate(stream: &mut TcpStream, code: u32) {
    stream.write_u32(8).await.unwrap();
    stream.write_u32(code).await.unwrap();
}
#[tokio::test]
async fn negotiation_is_bounded_and_gss_then_ssl_is_accepted() {
    let f = Fixture::new(TlsMode::Disabled, false, false).await;
    let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
    for code in [80877104, 80877103] {
        negotiate(&mut stream, code).await;
        assert_eq!(stream.read_u8().await.unwrap(), b'N');
    }
    let body = b"\0\x03\0\0user\0owner\0database\0postgres\0\0";
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
    assert_eq!(stream.read_u8().await.unwrap(), b'R');
    drop(stream);
    for codes in [vec![80877103, 80877103], vec![80877104, 80877104, 80877104]] {
        let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
        for (index, code) in codes.iter().enumerate() {
            negotiate(&mut stream, *code).await;
            if index + 1 != codes.len() {
                assert_eq!(stream.read_u8().await.unwrap(), b'N');
            }
        }
        assert_eq!(stream.read_u8().await.unwrap(), b'E');
        let length = stream.read_u32().await.unwrap();
        assert!((4..65536).contains(&length));
        let mut body = vec![0; length as usize - 4];
        stream.read_exact(&mut body).await.unwrap();
        assert!(body.split(|b| *b == 0).any(|field| field == b"C08P01"));
    }
    f.stop().await;
}

#[tokio::test]
async fn datagrip_refresh_extended_protocol_uses_current_backend_tls_state() {
    let f = Fixture::new(TlsMode::Optional, true, false).await;
    let plain = f.plain("owner", None).await;
    let secure = f.secure("owner", None, false).await;
    let mut pids = Vec::new();
    for (client, ssl) in [(&plain, false), (&secure, true)] {
        // Explicit prepare exercises Parse/Describe, then Bind/Execute.
        let statement = client
            .prepare(include_str!(
                "../../giggles-pg-catalog/tests/fixtures/datagrip_refresh/ssl.sql"
            ))
            .await
            .unwrap();
        let rows = client.query(&statement, &[]).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, bool>(0), ssl);
        let row = client.query_one("select pid, pg_backend_pid(), ssl, version, cipher, bits, client_dn, client_serial, issuer_dn from pg_stat_ssl", &[]).await.unwrap();
        let pid: i32 = row.get(0);
        assert!(pid > 0);
        assert_eq!(row.get::<_, i32>(1), pid);
        assert_eq!(row.get::<_, bool>(2), ssl);
        pids.push(pid);
        for sql in [
            include_str!("../../giggles-pg-catalog/tests/fixtures/datagrip_refresh/locks.sql"),
            include_str!(
                "../../giggles-pg-catalog/tests/fixtures/datagrip_refresh/user_mapping.sql"
            ),
        ] {
            let statement = client.prepare(sql).await.unwrap();
            assert!(client.query(&statement, &[]).await.unwrap().is_empty());
        }
        let sql = include_str!(
            "../../giggles-pg-catalog/tests/fixtures/datagrip_refresh/column_acls.sql"
        )
        .replace("?::oid", "$1::oid");
        let statement = client.prepare(&sql).await.unwrap();
        let rows = client.query(&statement, &[&2200u32]).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(statement.columns()[2].type_().oid(), 1034);
    }
    assert_ne!(pids[0], pids[1]);
    drop(plain);
    drop(secure);
    f.stop().await;
}
