use giggles_core::{Admin, Limits, roles::NewRole};
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
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

static SERIAL: AtomicU64 = AtomicU64::new(0);
const CATALOG: &str = "SELECT relname FROM pg_catalog.pg_class WHERE relnamespace=2200 AND relkind='r' ORDER BY relname LIMIT 100";

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
    async fn strings(self, client: &Client, sql: &str) -> Vec<String> {
        match self {
            Self::Simple => client
                .simple_query(sql)
                .await
                .unwrap()
                .into_iter()
                .filter_map(|m| {
                    if let SimpleQueryMessage::Row(row) = m {
                        Some(row.get(0).unwrap().to_owned())
                    } else {
                        None
                    }
                })
                .collect(),
            Self::Extended => client
                .query(sql, &[])
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.get(0))
                .collect(),
        }
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
            "role-sql-network-{}-{}-{}",
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
        let ca = certificate(None, false);
        let server = certificate(Some(&ca), true);
        let app = certificate(Some(&ca), false);
        let service = Service::new(
            admin.clone(),
            vec![Binding {
                fingerprint: tls::fingerprint(app.cert.der()),
                role: Role::Application {
                    domain: admin.authority_metadata().unwrap().0.to_string(),
                    tables: BTreeSet::from(["items".into()]),
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
            loopback_trust: false,
            connections: 32,
        };
        let listener = config.build().unwrap();
        let pg = serve_postgresql(config.listen, service.clone(), Arc::new(listener))
            .await
            .unwrap();
        Self {
            pg,
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
    async fn bad_password(&self, user: &str, password: &str) {
        let error = match self.config(user, password).connect(NoTls).await {
            Ok(_) => panic!("unexpected authentication success"),
            Err(error) => error,
        };
        assert_eq!(error.code().unwrap().code(), "28P01");
    }
}

async fn privileges(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol
        .ok(
            &owner,
            "CREATE TABLE items (id BIGINT PRIMARY KEY, body TEXT)",
        )
        .await;
    protocol
        .ok(&owner, "INSERT INTO items VALUES (1, 'owner-item')")
        .await;
    protocol
        .ok(&owner, "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'")
        .await;
    let alice = f.connect("alice", "alice-pw").await;
    protocol
        .error(&alice, "SELECT body FROM items LIMIT 1", "42501")
        .await;
    protocol
        .ok(&owner, "GRANT SELECT ON TABLE items TO alice")
        .await;
    assert_eq!(
        protocol
            .strings(&alice, "SELECT body FROM items LIMIT 1")
            .await,
        ["owner-item"]
    );
    protocol
        .error(&alice, "INSERT INTO items VALUES (2, 'forbidden')", "42501")
        .await;
    protocol
        .ok(&owner, "REVOKE SELECT ON TABLE items FROM alice")
        .await;
    protocol
        .error(&alice, "SELECT body FROM items LIMIT 1", "42501")
        .await;
    protocol
        .error(
            &alice,
            "CREATE TABLE mine (id BIGINT PRIMARY KEY, body TEXT)",
            "42501",
        )
        .await;
    protocol
        .ok(&owner, "GRANT CREATE ON SCHEMA public TO alice")
        .await;
    protocol
        .ok(
            &alice,
            "CREATE TABLE mine (id BIGINT PRIMARY KEY, body TEXT)",
        )
        .await;
    protocol
        .ok(&alice, "INSERT INTO mine VALUES (1, 'alice-item')")
        .await;
    assert_eq!(
        protocol
            .strings(&alice, "SELECT body FROM mine LIMIT 1")
            .await,
        ["alice-item"]
    );
    let visible = protocol.strings(&alice, CATALOG).await;
    assert!(visible.iter().any(|n| n == "mine"));
    assert!(!visible.iter().any(|n| n == "items"));
    let owner_visible = protocol.strings(&owner, CATALOG).await;
    assert!(owner_visible.iter().any(|n| n == "mine"));
    assert!(owner_visible.iter().any(|n| n == "items"));
    protocol.error(&owner, "DROP ROLE alice", "2BP01").await;
    protocol.ok(&owner, "DROP ROLE IF EXISTS nobody").await;
    protocol.error(&owner, "DROP ROLE owner", "55006").await;
    drop(alice);
    drop(owner);
    f.pg.shutdown().await.unwrap();
}

async fn passwords_and_authority(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol
        .ok(&owner, "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'")
        .await;
    let alice = f.connect("alice", "alice-pw").await;
    protocol.ok(&alice, "BEGIN").await;
    protocol.ok(&alice, "ROLLBACK").await;
    protocol
        .ok(&owner, "ALTER ROLE alice PASSWORD 'new-pw'")
        .await;
    assert!(protocol.execute(&alice, "BEGIN").await.is_err());
    f.bad_password("alice", "alice-pw").await;
    let alice = f.connect("alice", "new-pw").await;
    protocol
        .error(&alice, "ALTER ROLE alice SUPERUSER", "42501")
        .await;
    protocol.error(&alice, "CREATE ROLE bob", "42501").await;
    protocol
        .ok(&alice, "ALTER ROLE alice PASSWORD 'self-pw'")
        .await;
    assert!(protocol.execute(&alice, "BEGIN").await.is_err());
    drop(alice);
    f.bad_password("alice", "new-pw").await;
    let alice = f.connect("alice", "self-pw").await;
    protocol.ok(&alice, "BEGIN").await;
    protocol.ok(&alice, "ROLLBACK").await;
    protocol
        .ok(&owner, "CREATE ROLE mgr LOGIN CREATEROLE PASSWORD 'm'")
        .await;
    let mgr = f.connect("mgr", "m").await;
    protocol.ok(&mgr, "CREATE ROLE bob").await;
    protocol
        .error(&mgr, "CREATE ROLE root SUPERUSER", "42501")
        .await;
    protocol.ok(&owner, "BEGIN").await;
    protocol.error(&owner, "CREATE ROLE x", "25001").await;
    protocol
        .error(&owner, "GRANT CREATE ON SCHEMA public TO alice", "25001")
        .await;
    protocol.ok(&owner, "ROLLBACK").await;
    let verifier = Verifier::generate("v-pw");
    protocol
        .ok(
            &owner,
            &format!("CREATE ROLE v LOGIN PASSWORD '{verifier}'"),
        )
        .await;
    let v = f.connect("v", "v-pw").await;
    protocol.ok(&v, "BEGIN").await;
    protocol.ok(&v, "ROLLBACK").await;
    drop(v);
    drop(mgr);
    drop(alice);
    drop(owner);
    f.pg.shutdown().await.unwrap();
}

async fn unsupported(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol.ok(&owner, "CREATE ROLE alice").await;
    protocol
        .ok(&owner, "CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .await;
    protocol
        .error(&owner, "CREATE ROLE dbmaker CREATEDB", "0A000")
        .await;
    protocol
        .error(&owner, "GRANT SELECT ON TABLE items TO PUBLIC", "0A000")
        .await;
    protocol
        .error(&owner, "GRANT CREATE ON SCHEMA other TO alice", "3F000")
        .await;
    drop(owner);
    f.pg.shutdown().await.unwrap();
}

async fn application(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol
        .ok(&owner, "CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .await;
    protocol.ok(&owner, "CREATE ROLE alice").await;
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
    protocol.error(&app, "CREATE ROLE forbidden", "42501").await;
    protocol
        .error(&app, "GRANT SELECT ON TABLE items TO alice", "42501")
        .await;
    drop(app);
    drop(owner);
    agent.shutdown().await.unwrap();
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn simple_privileges_and_catalog() {
    privileges(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_privileges_and_catalog() {
    privileges(Protocol::Extended).await;
}
#[tokio::test]
async fn simple_passwords_and_role_authority() {
    passwords_and_authority(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_passwords_and_role_authority() {
    passwords_and_authority(Protocol::Extended).await;
}
#[tokio::test]
async fn simple_unsupported_role_features() {
    unsupported(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_unsupported_role_features() {
    unsupported(Protocol::Extended).await;
}
#[tokio::test]
async fn simple_application_cannot_manage_roles() {
    application(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_application_cannot_manage_roles() {
    application(Protocol::Extended).await;
}
