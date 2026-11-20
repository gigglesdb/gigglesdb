use giggles_core::{
    Admin, Limits,
    roles::{NewRole, SchemaPrivilege},
};
use giggles_server::{
    Binding, Role, Running, Service,
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
    clock::Clock,
    serve_pg, tls,
};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use rustls::pki_types::PrivatePkcs8KeyDer;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, Config, NoTls, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

static SERIAL: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Simple,
    Extended,
}
impl Protocol {
    async fn execute(self, client: &Client, sql: &str) -> Result<(), tokio_postgres::Error> {
        match self {
            Self::Simple => {
                client.simple_query(sql).await?;
            }
            Self::Extended => {
                client.execute(sql, &[]).await?;
            }
        }
        Ok(())
    }
    async fn ok(self, client: &Client, sql: &str) {
        self.execute(client, sql)
            .await
            .unwrap_or_else(|e| panic!("{self:?}: {sql}: {e}"));
    }
    async fn error(self, client: &Client, sql: &str, expected: &str) {
        let error = self.execute(client, sql).await.unwrap_err();
        assert_eq!(
            error.code().expect("server SQLSTATE").code(),
            expected,
            "{self:?}: {sql}: {error}"
        );
    }
}

struct Cert {
    cert: Certificate,
    key: KeyPair,
}
fn certificate(ca: Option<&Cert>, server: bool) -> Cert {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    if let Some(ca) = ca {
        params.extended_key_usages = vec![if server {
            ExtendedKeyUsagePurpose::ServerAuth
        } else {
            ExtendedKeyUsagePurpose::ClientAuth
        }];
        Cert {
            cert: params.signed_by(&key, &ca.cert, &ca.key).unwrap(),
            key,
        }
    } else {
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        Cert {
            cert: params.self_signed(&key).unwrap(),
            key,
        }
    }
}
struct Fixture {
    pg: Running,
    admin: Admin,
    service: Arc<Service>,
    ca: Cert,
    app: Cert,
    server: Cert,
}
impl Fixture {
    async fn new() -> Self {
        // Require the task environment rather than silently placing durable fixtures in user TEMP.
        let root = std::path::PathBuf::from(
            std::env::var_os("TEMP").expect("set task TEMP under J:/gigglesdb-work"),
        );
        assert!(
            root.to_string_lossy()
                .replace('\\', "/")
                .to_ascii_lowercase()
                .starts_with("j:/gigglesdb-work/")
        );
        let path = root.join(format!(
            "sequence-ddl-network-{}-{}-{}",
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
                login: true,
                superuser: true,
                password: Some(Verifier::generate("owner-pw")),
                ..NewRole::default()
            })
            .unwrap();
        for name in ["alice", "bob", "outsider"] {
            admin
                .roles()
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    password: Some(Verifier::generate("pw")),
                    ..NewRole::default()
                })
                .unwrap();
        }
        let ca = certificate(None, false);
        let server = certificate(Some(&ca), true);
        let app = certificate(Some(&ca), false);
        let service = Service::new(
            admin.clone(),
            vec![Binding {
                fingerprint: tls::fingerprint(app.cert.der()),
                role: Role::Application {
                    domain: admin.authority_metadata().unwrap().0.to_string(),
                    tables: BTreeSet::from(["items".into(), "app_named".into()]),
                },
            }],
            Clock::new(0, 3600).unwrap(),
        )
        .unwrap();
        let config = PostgresqlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: TlsMode::Disabled,
            certificate: None,
            private_key: None,
            client_ca: None,
            certificates: BTreeMap::new(),
            databases: vec!["postgres".into()],
            loopback_trust: true,
            connections: 32,
        };
        let listener = config.build().unwrap();
        let pg = serve_postgresql(config.listen, service.clone(), Arc::new(listener))
            .await
            .unwrap();
        Self {
            pg,
            admin,
            service,
            ca,
            app,
            server,
        }
    }
    fn config(&self, user: &str, password: &str) -> Config {
        let mut config = Config::new();
        config
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
            .password(password)
            .dbname("postgres")
            .ssl_mode(SslMode::Disable);
        config
    }
    async fn connect(&self, user: &str, password: &str) -> Client {
        let (client, connection) = self.config(user, password).connect(NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
}

async fn raw_connect(address: std::net::SocketAddr, user: &str) -> tokio::net::TcpStream {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend_from_slice(format!("user\0{user}\0database\0postgres\0\0").as_bytes());
    stream.write_u32((startup.len() + 4) as u32).await.unwrap();
    stream.write_all(&startup).await.unwrap();
    loop {
        let kind = stream.read_u8().await.unwrap();
        let length = stream.read_u32().await.unwrap();
        let mut body = vec![0; length as usize - 4];
        stream.read_exact(&mut body).await.unwrap();
        assert_ne!(kind, b'E', "startup: {body:?}");
        if kind == b'Z' {
            break;
        }
    }
    stream
}
async fn raw_query(
    protocol: Protocol,
    address: std::net::SocketAddr,
    sql: &str,
) -> Vec<(u8, Vec<u8>)> {
    let mut stream = raw_connect(address, "owner").await;
    raw_execute(protocol, &mut stream, sql).await
}
async fn raw_execute(
    protocol: Protocol,
    stream: &mut tokio::net::TcpStream,
    sql: &str,
) -> Vec<(u8, Vec<u8>)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let frames = match protocol {
        Protocol::Simple => {
            let mut body = sql.as_bytes().to_vec();
            body.push(0);
            vec![(b'Q', body)]
        }
        Protocol::Extended => {
            let mut parse = vec![0];
            parse.extend_from_slice(sql.as_bytes());
            parse.extend_from_slice(&[0, 0, 0]);
            vec![
                (b'P', parse),
                (b'B', vec![0; 8]),
                (b'E', vec![0; 5]),
                (b'S', vec![]),
            ]
        }
    };
    for (kind, body) in frames {
        stream.write_u8(kind).await.unwrap();
        stream.write_u32((body.len() + 4) as u32).await.unwrap();
        stream.write_all(&body).await.unwrap();
    }
    let mut messages = vec![];
    loop {
        let kind = stream.read_u8().await.unwrap();
        let length = stream.read_u32().await.unwrap();
        let mut body = vec![0; length as usize - 4];
        stream.read_exact(&mut body).await.unwrap();
        messages.push((kind, body));
        if kind == b'Z' {
            break;
        }
    }
    messages
}
async fn raw_tag(address: std::net::SocketAddr, sql: &str) -> String {
    let messages = raw_query(Protocol::Simple, address, sql).await;
    let (_, body) = messages
        .iter()
        .find(|(kind, _)| *kind == b'C')
        .unwrap_or_else(|| panic!("{sql}: {messages:?}"));
    String::from_utf8(body[..body.len() - 1].to_vec()).unwrap()
}

async fn sequence_ddl(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    let alice = f.connect("alice", "pw").await;
    let outsider = f.connect("outsider", "pw").await;
    protocol
        .error(&alice, "CREATE SEQUENCE denied", "42501")
        .await;
    f.admin
        .roles()
        .grant_schema("public", "alice", &[SchemaPrivilege::Create])
        .unwrap();
    protocol
        .ok(&alice, "CREATE SEQUENCE standalone START 5 INCREMENT 2")
        .await;
    assert_eq!(
        f.admin
            .roles()
            .table_owner("standalone")
            .unwrap()
            .as_deref(),
        Some("alice")
    );
    for function in ["nextval", "currval", "setval"] {
        let args = if function == "setval" { ", 10" } else { "" };
        for sequence in ["standalone", "absent"] {
            protocol
                .error(
                    &outsider,
                    &format!("SELECT {function}('{sequence}'{args})"),
                    "42501",
                )
                .await;
        }
    }
    let row = alice
        .query_one("SELECT nextval('standalone')", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 5);
    protocol
        .ok(&alice, "SELECT setval('standalone', 10, false)")
        .await;
    protocol.ok(&alice, "SELECT currval('standalone')").await;
    for sequence in ["standalone", "absent"] {
        protocol
            .error(
                &outsider,
                &format!("ALTER SEQUENCE {sequence} RESTART"),
                "42501",
            )
            .await;
        let sql = format!("DROP SEQUENCE IF EXISTS {sequence}");
        if sequence == "absent" { protocol.ok(&outsider, &sql).await; }
        else { protocol.error(&outsider, &sql, "42501").await; }
    }
    if matches!(protocol, Protocol::Extended) {
        let dynamic = alice.prepare("SELECT nextval($1)").await.unwrap();
        alice.query_one(&dynamic, &[&"standalone"]).await.unwrap();
        let prepared = alice
            .prepare("ALTER SEQUENCE standalone RESTART")
            .await
            .unwrap();
        f.admin
            .roles()
            .set_table_owner("standalone", "outsider")
            .unwrap();
        let error = alice.execute(&prepared, &[]).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "42501");
        let error = alice
            .query_one(&dynamic, &[&"standalone"])
            .await
            .unwrap_err();
        assert_eq!(error.code().unwrap().code(), "42501");
        f.admin
            .roles()
            .set_table_owner("standalone", "alice")
            .unwrap();
    }
    protocol.ok(&owner, "CREATE SEQUENCE owner_secret").await;
    protocol
        .ok(&alice, "CREATE SEQUENCE IF NOT EXISTS owner_secret")
        .await;
    assert_eq!(
        f.admin
            .roles()
            .table_owner("owner_secret")
            .unwrap()
            .as_deref(),
        Some("owner")
    );
    protocol
        .ok(&owner, "ALTER SEQUENCE standalone RESTART WITH 20")
        .await;
    assert_eq!(
        f.admin
            .roles()
            .table_owner("standalone")
            .unwrap()
            .as_deref(),
        Some("alice")
    );
    protocol.ok(&owner, "DROP SEQUENCE owner_secret").await;
    assert_eq!(f.admin.roles().table_owner("owner_secret").unwrap(), None);
    protocol.ok(&owner, "CREATE TABLE secret (id int)").await;
    protocol
        .ok(&alice, "CREATE SEQUENCE IF NOT EXISTS secret")
        .await;
    assert_eq!(
        f.admin.roles().table_owner("secret").unwrap().as_deref(),
        Some("owner")
    );
    protocol
        .error(
            &alice,
            "ALTER SEQUENCE standalone OWNED BY secret.id",
            "42501",
        )
        .await;
    protocol
        .error(&alice, "CREATE SEQUENCE probe OWNED BY absent.id", "42501")
        .await;
    protocol.ok(&alice, "CREATE TABLE items (id int)").await;
    protocol
        .ok(&alice, "ALTER SEQUENCE standalone OWNED BY items.id")
        .await;
    protocol.ok(&alice, "SELECT nextval('standalone')").await;
    protocol
        .ok(&alice, "ALTER SEQUENCE standalone OWNED BY NONE")
        .await;
    for sql in [
        "ALTER SEQUENCE standalone RENAME TO renamed",
        "ALTER SEQUENCE standalone OWNER TO outsider",
        "ALTER SEQUENCE standalone SET SCHEMA public",
    ] {
        protocol.error(&alice, sql, "0A000").await;
    }
    let error = protocol
        .execute(&alice, "CREATE SEQUENCE standalone")
        .await
        .unwrap_err();
    let db = error.as_db_error().unwrap();
    assert_eq!(db.code().code(), "42P07");

    let error = protocol
        .execute(&owner, "CREATE SEQUENCE invalid INCREMENT 0")
        .await
        .unwrap_err();
    let db = error.as_db_error().unwrap();
    assert_eq!(db.code().code(), "22023");

    protocol
        .ok(
            &alice,
            "CREATE TABLE dependency (id int DEFAULT nextval('standalone'))",
        )
        .await;
    let error = protocol
        .execute(&alice, "DROP SEQUENCE standalone")
        .await
        .unwrap_err();
    let db = error.as_db_error().unwrap();
    assert_eq!(db.code().code(), "2BP01");

    protocol
        .ok(&owner, "DROP SEQUENCE standalone CASCADE")
        .await;
    assert_eq!(f.admin.roles().table_owner("standalone").unwrap(), None);
    protocol
        .ok(&alice, "CREATE SEQUENCE transactional_owner")
        .await;
    protocol.ok(&alice, "BEGIN").await;
    protocol
        .ok(&alice, "DROP SEQUENCE transactional_owner")
        .await;
    assert_eq!(
        f.admin
            .roles()
            .table_owner("transactional_owner")
            .unwrap()
            .as_deref(),
        Some("alice")
    );
    protocol.ok(&alice, "ROLLBACK").await;
    assert_eq!(
        f.admin
            .roles()
            .table_owner("transactional_owner")
            .unwrap()
            .as_deref(),
        Some("alice")
    );
    protocol.ok(&alice, "BEGIN").await;
    protocol.ok(&alice, "SAVEPOINT owner_checkpoint").await;
    protocol
        .ok(&alice, "DROP SEQUENCE transactional_owner")
        .await;
    protocol.ok(&alice, "ROLLBACK TO owner_checkpoint").await;
    protocol.ok(&alice, "COMMIT").await;
    assert_eq!(
        f.admin
            .roles()
            .table_owner("transactional_owner")
            .unwrap()
            .as_deref(),
        Some("alice")
    );
    protocol.ok(&alice, "BEGIN").await;
    protocol
        .ok(&alice, "DROP SEQUENCE transactional_owner")
        .await;
    assert_eq!(
        f.admin
            .roles()
            .table_owner("transactional_owner")
            .unwrap()
            .as_deref(),
        Some("alice")
    );
    protocol.ok(&alice, "COMMIT").await;
    assert_eq!(
        f.admin.roles().table_owner("transactional_owner").unwrap(),
        None
    );
    protocol
        .ok(&alice, "CREATE TABLE serial_creator (id serial)")
        .await;
    assert_eq!(
        f.admin
            .roles()
            .table_owner("serial_creator_id_seq")
            .unwrap()
            .as_deref(),
        Some("alice")
    );
    protocol
        .ok(
            &alice,
            "ALTER SEQUENCE serial_creator_id_seq RESTART WITH 50",
        )
        .await;
    protocol
        .ok(&alice, "DROP SEQUENCE serial_creator_id_seq CASCADE")
        .await;
    assert_eq!(
        f.admin
            .roles()
            .table_owner("serial_creator_id_seq")
            .unwrap(),
        None
    );
    protocol
        .ok(&alice, "INSERT INTO serial_creator VALUES (99)")
        .await;
    protocol.ok(&alice, "CREATE SEQUENCE reused_relation").await;
    protocol.ok(&alice, "BEGIN").await;
    protocol.ok(&alice, "DROP SEQUENCE reused_relation").await;
    protocol
        .ok(&alice, "CREATE TABLE reused_relation (id int)")
        .await;
    protocol.ok(&alice, "COMMIT").await;
    assert_eq!(
        f.admin
            .roles()
            .table_owner("reused_relation")
            .unwrap()
            .as_deref(),
        Some("alice")
    );
    protocol.ok(&alice, "SELECT * FROM reused_relation").await;
    protocol
        .ok(&alice, "CREATE SEQUENCE table_owned OWNED BY items.id")
        .await;
    protocol.ok(&owner, "DROP TABLE items").await;
    assert_eq!(f.admin.roles().table_owner("table_owned").unwrap(), None);
    // The command strings are the PostgreSQL tags, including the object kind.
    for (sql, tag) in [
        ("CREATE SEQUENCE tagged", "CREATE SEQUENCE"),
        ("ALTER SEQUENCE tagged RESTART WITH 40", "ALTER SEQUENCE"),
        ("DROP SEQUENCE tagged", "DROP SEQUENCE"),
    ] {
        assert_eq!(raw_tag(f.pg.address, sql).await, tag);
    }
    // Backlog: exact PG error and notice text via a designed side channel.
    for (sql, tag) in [
        ("CREATE SEQUENCE IF NOT EXISTS secret", "CREATE SEQUENCE"),
        ("DROP SEQUENCE IF EXISTS absent_notice", "DROP SEQUENCE"),
        (
            "ALTER SEQUENCE IF EXISTS absent_notice RESTART",
            "ALTER SEQUENCE",
        ),
    ] {
        let messages = raw_query(protocol, f.pg.address, sql).await;
        assert_eq!(
            messages.iter().any(|(kind, _)| *kind == b'N'),
            sql == "DROP SEQUENCE IF EXISTS absent_notice",
            "{messages:?}"
        );
        let (_, body) = messages
            .iter()
            .find(|(kind, _)| *kind == b'C')
            .unwrap_or_else(|| panic!("{sql}: {messages:?}"));
        assert_eq!(&body[..body.len() - 1], tag.as_bytes(), "{sql}");
    }
    protocol.ok(&owner, "CREATE SEQUENCE app_named").await;
    let server_tls = tls::server(
        vec![f.server.cert.der().clone()],
        PrivatePkcs8KeyDer::from(f.server.key.serialize_der()).into(),
        vec![f.ca.cert.der().clone()],
    )
    .unwrap();
    let agent = serve_pg(
        "127.0.0.1:0".parse().unwrap(),
        f.service.clone(),
        server_tls,
        8,
    )
    .await
    .unwrap();
    let client_tls = tls::client(
        vec![f.app.cert.der().clone()],
        PrivatePkcs8KeyDer::from(f.app.key.serialize_der()).into(),
        vec![f.ca.cert.der().clone()],
    )
    .unwrap();
    let mut config = Config::new();
    config
        .host("localhost")
        .hostaddr(agent.address.ip())
        .port(agent.address.port())
        .user("giggles")
        .dbname("giggles")
        .ssl_mode(SslMode::Require);
    let (app, connection) = config
        .connect(MakeRustlsConnect::new((*client_tls).clone()))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    protocol
        .error(&app, "SELECT nextval('app_named')", "42501")
        .await;
    protocol
        .error(&app, "CREATE SEQUENCE forbidden", "42501")
        .await;
    protocol
        .error(&app, "ALTER SEQUENCE absent RESTART", "42501")
        .await;
    drop(app);
    f.service
        .reload_bindings(vec![Binding {
            fingerprint: tls::fingerprint(f.app.cert.der()),
            role: Role::NoSql { domain: f.admin.authority_metadata().unwrap().0.to_string() },
        }])
        .unwrap();
    let (task, connection) = config
        .connect(MakeRustlsConnect::new((*client_tls).clone()))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for function in ["nextval", "currval", "setval"] {
        for sequence in ["items_id_seq", "absent_seq"] {
            let args = if function == "setval" { ", 1" } else { "" };
            protocol
                .error(
                    &task,
                    &format!("SELECT {function}('{sequence}'{args})"),
                    "42501",
                )
                .await;
        }
    }
    for sql in [
        "CREATE SEQUENCE denied",
        "ALTER SEQUENCE absent RESTART",
        "DROP SEQUENCE IF EXISTS absent",
    ] {
        protocol.error(&task, sql, "42501").await;
    }
    drop(task);
    drop(outsider);
    drop(alice);
    drop(owner);
    agent.shutdown().await.unwrap();
    f.pg.shutdown().await.unwrap();
}
#[tokio::test]
async fn simple_sequence_ddl_authority() {
    sequence_ddl(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_sequence_ddl_authority() {
    sequence_ddl(Protocol::Extended).await;
}

// W1-W4: ownership must follow the committed relation, including after name reuse.
async fn sequence_owner_name_reuse(protocol: Protocol) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let f = Fixture::new().await;
    for role in ["alice", "bob"] {
        f.admin
            .roles()
            .grant_schema("public", role, &[SchemaPrivilege::Create])
            .unwrap();
    }
    let alice = f.connect("alice", "pw").await;
    let bob = f.connect("bob", "pw").await;
    let outsider = f.connect("outsider", "pw").await;

    // W1: rolled-back sequence name reused by a different sequence owner.
    protocol.ok(&alice, "BEGIN").await;
    protocol.ok(&alice, "CREATE SEQUENCE s").await;
    protocol.ok(&alice, "ROLLBACK").await;
    protocol.ok(&bob, "CREATE SEQUENCE s").await;
    assert_sequence_owner_actions(protocol, &f, &alice, &bob, "s").await;
    eprintln!("W1 {protocol:?}: PASS");

    // W2: the same name can instead become an ordinary table.
    protocol.ok(&alice, "BEGIN").await;
    protocol.ok(&alice, "CREATE SEQUENCE s").await;
    protocol.ok(&alice, "ROLLBACK").await;
    protocol.ok(&bob, "CREATE TABLE s (a int)").await;
    protocol.ok(&bob, "INSERT INTO s VALUES (7)").await;
    assert_eq!(
        f.admin.roles().table_owner("s").unwrap().as_deref(),
        Some("bob")
    );
    protocol.ok(&bob, "SELECT * FROM s").await;
    protocol.error(&alice, "SELECT * FROM s", "42501").await;
    protocol.ok(&bob, "DROP TABLE s").await;
    eprintln!("W2 {protocol:?}: PASS");

    // W3: a stale owner key cannot manufacture a relation or survive wire creation.
    f.admin.roles().set_table_owner("ghost", "alice").unwrap();
    for sql in ["SELECT nextval('ghost')", "ALTER SEQUENCE ghost RESTART"] {
        let error = protocol.execute(&alice, sql).await.unwrap_err();
        let code = error.code().expect("server SQLSTATE").code();
        eprintln!("W3 {protocol:?}: {sql}: {code}");
        assert!(matches!(code, "42P01" | "42501"), "{error}");
    }
    for sql in [
        "SELECT nextval('ghost')",
        "ALTER SEQUENCE ghost RESTART",
        "DROP SEQUENCE ghost",
    ] {
        let expected = if sql == "DROP SEQUENCE ghost" { "42P01" } else { "42501" };
        protocol.error(&outsider, sql, expected).await;
    }
    protocol.ok(&bob, "CREATE SEQUENCE ghost").await;
    for sql in [
        "SELECT nextval('ghost')",
        "ALTER SEQUENCE ghost RESTART",
        "DROP SEQUENCE ghost",
    ] {
        protocol.error(&outsider, sql, "42501").await;
    }
    assert_sequence_owner_actions(protocol, &f, &alice, &bob, "ghost").await;
    eprintln!("W3 {protocol:?}: PASS");

    // W4: half-close the client write side and observe server EOF. The server
    // drops its session (and transaction) before its socket closes; no timer
    // or client-driver completion is used as a cleanup acknowledgement.
    let mut abandoned = raw_connect(f.pg.address, "alice").await;
    for sql in ["BEGIN", "CREATE SEQUENCE s2"] {
        let messages = raw_execute(protocol, &mut abandoned, sql).await;
        assert!(
            !messages.iter().any(|(kind, _)| *kind == b'E'),
            "{sql}: {messages:?}"
        );
        assert!(
            messages.iter().any(|(kind, _)| *kind == b'C'),
            "{sql}: {messages:?}"
        );
    }
    abandoned.shutdown().await.unwrap();
    let mut tail = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        abandoned.read_to_end(&mut tail),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(tail.is_empty(), "unexpected disconnect response: {tail:?}");
    protocol.ok(&bob, "CREATE SEQUENCE s2").await;
    assert_sequence_owner_actions(protocol, &f, &alice, &bob, "s2").await;
    eprintln!("W4 {protocol:?}: PASS");

    drop(outsider);
    drop(alice);
    drop(bob);
    f.pg.shutdown().await.unwrap();
}

async fn assert_sequence_owner_actions(
    protocol: Protocol,
    f: &Fixture,
    alice: &Client,
    bob: &Client,
    name: &str,
) {
    assert_eq!(
        f.admin.roles().table_owner(name).unwrap().as_deref(),
        Some("bob")
    );
    protocol.ok(bob, &format!("SELECT nextval('{name}')")).await;
    protocol
        .ok(bob, &format!("ALTER SEQUENCE {name} RESTART WITH 20"))
        .await;
    for sql in [
        format!("SELECT nextval('{name}')"),
        format!("ALTER SEQUENCE {name} RESTART"),
        format!("DROP SEQUENCE {name}"),
    ] {
        protocol.error(alice, &sql, "42501").await;
    }
    protocol.ok(bob, &format!("DROP SEQUENCE {name}")).await;
}

#[tokio::test]
async fn simple_sequence_owner_name_reuse() {
    sequence_owner_name_reuse(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_sequence_owner_name_reuse() {
    sequence_owner_name_reuse(Protocol::Extended).await;
}

async fn owner_key_hygiene(protocol: Protocol) {
    use giggles_core::roles::TablePrivilege;
    let f = Fixture::new().await;
    f.admin
        .roles()
        .create(NewRole {
            name: "carol".into(),
            login: true,
            password: Some(Verifier::generate("pw")),
            ..NewRole::default()
        })
        .unwrap();
    f.admin
        .roles()
        .grant_schema("public", "carol", &[SchemaPrivilege::Create])
        .unwrap();
    let alice = f.connect("alice", "pw").await;
    let bob = f.connect("bob", "pw").await;
    let carol = f.connect("carol", "pw").await;
    let roles = f.admin.roles();

    // Native creation cannot inherit absent-object owner or grant keys.
    roles.set_table_owner("ghost2", "alice").unwrap();
    roles
        .grant_table("ghost2", "bob", &[TablePrivilege::Select])
        .unwrap();
    let mut native = f.admin.sql_session();
    native
        .execute("CREATE TABLE ghost2 (a int)", &[], None)
        .unwrap();
    native
        .execute("INSERT INTO ghost2 VALUES (731)", &[], None)
        .unwrap();
    for client in [&alice, &bob] {
        protocol
            .error(client, "SELECT a FROM ghost2", "42501")
            .await;
    }
    assert_eq!(roles.table_owner("ghost2").unwrap(), None);
    assert!(roles.table_privileges("ghost2", "bob").unwrap().is_empty());

    // R: a rename replaces absent-destination keys with the source authority.
    roles.set_table_owner("renamed", "alice").unwrap();
    roles
        .grant_table("renamed", "bob", &[TablePrivilege::Select])
        .unwrap();
    protocol.ok(&carol, "CREATE TABLE src (a int)").await;
    protocol.ok(&carol, "INSERT INTO src VALUES (3)").await;
    protocol
        .ok(&carol, "ALTER TABLE src RENAME TO renamed")
        .await;
    protocol.ok(&carol, "SELECT * FROM renamed").await;
    assert_eq!(
        roles.table_owner("renamed").unwrap().as_deref(),
        Some("carol")
    );
    for client in [&alice, &bob] {
        protocol
            .error(client, "SELECT * FROM renamed", "42501")
            .await;
    }

    // B: a rolled-back SQL creator cannot read a later native relation.
    roles
        .create(NewRole {
            name: "dave".into(),
            login: true,
            password: Some(Verifier::generate("pw")),
            ..NewRole::default()
        })
        .unwrap();
    roles
        .grant_schema("public", "dave", &[SchemaPrivilege::Create])
        .unwrap();
    let dave = f.connect("dave", "pw").await;
    protocol.ok(&dave, "BEGIN").await;
    protocol.ok(&dave, "CREATE TABLE rb (a int)").await;
    protocol.ok(&dave, "ROLLBACK").await;
    native
        .execute("CREATE TABLE rb (a int)", &[], None)
        .unwrap();
    native
        .execute("INSERT INTO rb VALUES (5)", &[], None)
        .unwrap();
    assert_eq!(roles.table_owner("rb").unwrap(), None);
    let mut raw_dave = raw_connect(f.pg.address, "dave").await;
    let messages = raw_execute(protocol, &mut raw_dave, "SELECT a FROM rb").await;
    let (_, error) = messages
        .iter()
        .find(|(kind, _)| *kind == b'E')
        .unwrap_or_else(|| panic!("{protocol:?}: SELECT a FROM rb: {messages:?}"));
    assert!(
        error
            .split(|byte| *byte == 0)
            .any(|field| field == b"C42501"),
        "{protocol:?}: SELECT a FROM rb: {messages:?}"
    );
    assert!(
        !messages.iter().any(|(kind, _)| *kind == b'D'),
        "{protocol:?}: dave received rows from rb: {messages:?}"
    );
    drop(raw_dave);
    drop(dave);

    // Managed core commit must retain the owner installed by the SQL service.
    protocol.ok(&carol, "BEGIN").await;
    protocol.ok(&carol, "CREATE TABLE x (a int)").await;
    protocol.ok(&carol, "COMMIT").await;
    assert_eq!(roles.table_owner("x").unwrap().as_deref(), Some("carol"));
    protocol.ok(&carol, "SELECT * FROM x").await;

    roles.set_table_owner("y", "alice").unwrap();
    roles
        .grant_table("y", "bob", &[TablePrivilege::Select])
        .unwrap();
    roles.set_table_owner("y_id_seq", "alice").unwrap();
    roles
        .grant_table("y_id_seq", "bob", &[TablePrivilege::Select])
        .unwrap();
    let mut managed = f.service.role_session("carol").unwrap();
    managed
        .execute_sql("CREATE TABLE y (id serial)", &[])
        .unwrap();
    protocol.error(&bob, "SELECT * FROM y", "42501").await;
    assert_eq!(roles.table_owner("y").unwrap().as_deref(), Some("carol"));
    assert_eq!(
        roles.table_owner("y_id_seq").unwrap().as_deref(),
        Some("carol")
    );
    assert!(
        roles
            .table_privileges("y_id_seq", "bob")
            .unwrap()
            .is_empty()
    );

    // Existing relation no-ops preserve deliberately installed authority keys.
    roles
        .grant_table("y", "bob", &[TablePrivilege::Select])
        .unwrap();
    protocol
        .ok(&carol, "CREATE TABLE IF NOT EXISTS y (a int)")
        .await;
    assert_eq!(roles.table_owner("y").unwrap().as_deref(), Some("carol"));
    assert!(
        roles
            .table_privileges("y", "bob")
            .unwrap()
            .contains(&TablePrivilege::Select)
    );
    protocol.ok(&bob, "SELECT * FROM y").await;
    protocol.ok(&carol, "CREATE SEQUENCE keep_seq").await;
    roles
        .grant_table("keep_seq", "bob", &[TablePrivilege::Select])
        .unwrap();
    protocol
        .ok(&carol, "CREATE SEQUENCE IF NOT EXISTS keep_seq")
        .await;
    assert_eq!(
        roles.table_owner("keep_seq").unwrap().as_deref(),
        Some("carol")
    );
    assert!(
        roles
            .table_privileges("keep_seq", "bob")
            .unwrap()
            .contains(&TablePrivilege::Select)
    );
    // Both CREATE forms share a relation namespace across table/sequence kinds.
    protocol
        .ok(&carol, "CREATE TABLE IF NOT EXISTS keep_seq (a int)")
        .await;
    protocol.ok(&carol, "CREATE SEQUENCE IF NOT EXISTS y").await;
    for name in ["keep_seq", "y"] {
        assert_eq!(roles.table_owner(name).unwrap().as_deref(), Some("carol"));
        assert!(
            roles
                .table_privileges(name, "bob")
                .unwrap()
                .contains(&TablePrivilege::Select)
        );
    }
    drop(managed);
    drop(native);
    drop(alice);
    drop(bob);
    drop(carol);
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn simple_owner_key_hygiene() {
    owner_key_hygiene(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_owner_key_hygiene() {
    owner_key_hygiene(Protocol::Extended).await;
}

#[tokio::test]
async fn extension_scalars_route_through_database_context() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    const HINT: &str = "No function matches the given name and argument types. You might need to add explicit type casts.";
    async fn missing(client: &Client, sql: &str, message: &str) {
        let error = client.simple_query(sql).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "42883", "{sql}");
        assert_eq!(error.message(), message);
        assert_eq!(error.hint(), Some(HINT));
    }
    missing(
        &owner,
        "SELECT uuid_nil()",
        "function uuid_nil() does not exist",
    )
    .await;
    owner
        .simple_query("CREATE EXTENSION \"uuid-ossp\"")
        .await
        .unwrap();
    for (sql, expected) in [
        (
            "SELECT uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8','example.com')",
            vec!["cfbff0d1-9375-5685-968c-48ce8b15ae17"],
        ),
        (
            "SELECT uuid_generate_v3(uuid_ns_url(), 'http://x'), uuid_nil(), uuid_ns_dns(), uuid_ns_oid(), uuid_ns_x500()",
            vec![
                "53b14841-9895-312b-bb91-82c1f08c9edd",
                "00000000-0000-0000-0000-000000000000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                "6ba7b812-9dad-11d1-80b4-00c04fd430c8",
                "6ba7b814-9dad-11d1-80b4-00c04fd430c8",
            ],
        ),
        (
            "SELECT length(uuid_generate_v4()::text), uuid_generate_v1() IS NOT NULL",
            vec!["36", "t"],
        ),
        ("SELECT pg_typeof(uuid_nil())", vec!["uuid"]),
    ] {
        let messages = owner.simple_query(sql).await.unwrap();
        let row = messages
            .iter()
            .find_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            (0..row.len())
                .map(|i| row.get(i).unwrap())
                .collect::<Vec<_>>(),
            expected,
            "{sql}"
        );
        if sql.starts_with("SELECT uuid_generate_v5") {
            assert_eq!(row.columns()[0].name(), "uuid_generate_v5");
        }
    }
    // Typed Parse UUID/TEXT, text-format Bind values, and text-format result.
    let mut stream = raw_connect(f.pg.address, "owner").await;
    let mut parse = b"\0SELECT uuid_generate_v5($1,$2)\0".to_vec();
    parse.extend_from_slice(&2u16.to_be_bytes());
    parse.extend_from_slice(&2950u32.to_be_bytes());
    parse.extend_from_slice(&25u32.to_be_bytes());
    let mut bind = vec![0, 0, 0, 0, 0, 2];
    for value in ["6ba7b810-9dad-11d1-80b4-00c04fd430c8", "example.com"] {
        bind.extend_from_slice(&(value.len() as u32).to_be_bytes());
        bind.extend_from_slice(value.as_bytes());
    }
    bind.extend_from_slice(&[0, 0]);
    for (kind, body) in [
        (b'P', parse),
        (b'B', bind),
        (b'E', vec![0; 5]),
        (b'S', vec![]),
    ] {
        stream.write_u8(kind).await.unwrap();
        stream.write_u32((body.len() + 4) as u32).await.unwrap();
        stream.write_all(&body).await.unwrap();
    }
    let mut rows = 0;
    loop {
        let kind = stream.read_u8().await.unwrap();
        let len = stream.read_u32().await.unwrap();
        let mut body = vec![0; len as usize - 4];
        stream.read_exact(&mut body).await.unwrap();
        assert_ne!(kind, b'E', "{body:?}");
        if kind == b'D' {
            assert_eq!(&body[6..], b"cfbff0d1-9375-5685-968c-48ce8b15ae17");
            rows += 1;
        }
        if kind == b'Z' {
            break;
        }
    }
    assert_eq!(rows, 1);
    drop(stream);
    // Task admission must still run before extension binding, on both protocols.
    f.service
        .reload_bindings(vec![Binding {
            fingerprint: tls::fingerprint(f.app.cert.der()),
            role: Role::NoSql { domain: f.admin.authority_metadata().unwrap().0.to_string() },
        }])
        .unwrap();
    let server_tls = tls::server(
        vec![f.server.cert.der().clone()],
        PrivatePkcs8KeyDer::from(f.server.key.serialize_der()).into(),
        vec![f.ca.cert.der().clone()],
    )
    .unwrap();
    let agent = serve_pg(
        "127.0.0.1:0".parse().unwrap(),
        f.service.clone(),
        server_tls,
        8,
    )
    .await
    .unwrap();
    let client_tls = tls::client(
        vec![f.app.cert.der().clone()],
        PrivatePkcs8KeyDer::from(f.app.key.serialize_der()).into(),
        vec![f.ca.cert.der().clone()],
    )
    .unwrap();
    let (task, connection) = Config::new()
        .host("localhost")
        .hostaddr(agent.address.ip())
        .port(agent.address.port())
        .user("giggles")
        .dbname("giggles")
        .ssl_mode(SslMode::Require)
        .connect(MakeRustlsConnect::new((*client_tls).clone()))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for protocol in [Protocol::Simple, Protocol::Extended] {
        protocol.error(&task, "SELECT uuid_nil()", "42501").await;
    }
    owner
        .simple_query("DROP EXTENSION \"uuid-ossp\"")
        .await
        .unwrap();
    missing(
        &owner,
        "SELECT uuid_nil()",
        "function uuid_nil() does not exist",
    )
    .await;
    for protocol in [Protocol::Simple, Protocol::Extended] {
        protocol.error(&task, "SELECT uuid_nil()", "42501").await;
    }
    drop(task);
    drop(owner);
    agent.shutdown().await.unwrap();
    f.pg.shutdown().await.unwrap();
}
