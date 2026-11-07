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
            "drop-table-network-{}-{}-{}",
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
}

async fn lifecycle(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol
        .ok(&owner, "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)")
        .await;
    protocol
        .ok(&owner, "INSERT INTO t VALUES (1, 'old-row')")
        .await;
    assert_eq!(
        protocol.strings(&owner, "SELECT body FROM t LIMIT 1").await,
        ["old-row"]
    );
    assert!(
        protocol
            .strings(&owner, CATALOG)
            .await
            .iter()
            .any(|n| n == "t")
    );
    protocol.ok(&owner, "DROP TABLE public.t;").await;
    assert!(
        !protocol
            .strings(&owner, CATALOG)
            .await
            .iter()
            .any(|n| n == "t")
    );
    protocol
        .error(&owner, "SELECT body FROM t LIMIT 1", "42P01")
        .await;
    protocol
        .ok(&owner, "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)")
        .await;
    assert!(
        protocol
            .strings(&owner, "SELECT body FROM t LIMIT 1")
            .await
            .is_empty()
    );
    protocol
        .ok(&owner, "CREATE TABLE t2 (id BIGINT PRIMARY KEY)")
        .await;
    protocol.ok(&owner, "DROP TABLE t2").await;
    protocol
        .error(&owner, "SELECT id FROM t2 LIMIT 1", "42P01")
        .await;
    drop(owner);
    f.pg.shutdown().await.unwrap();
}

async fn validation(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol.ok(&owner, "DROP TABLE IF EXISTS missing").await;
    protocol.error(&owner, "DROP TABLE missing", "42P01").await;
    protocol
        .ok(&owner, "CREATE TABLE a (id BIGINT PRIMARY KEY, body TEXT)")
        .await;
    protocol
        .ok(&owner, "INSERT INTO a VALUES (1, 'kept')")
        .await;
    protocol
        .error(&owner, "DROP TABLE a, missing", "42P01")
        .await;
    assert_eq!(
        protocol.strings(&owner, "SELECT body FROM a LIMIT 1").await,
        ["kept"]
    );
    protocol.ok(&owner, "DROP TABLE IF EXISTS missing, a").await;
    protocol
        .error(&owner, "SELECT body FROM a LIMIT 1", "42P01")
        .await;
    protocol
        .ok(&owner, "CREATE TABLE a (id BIGINT PRIMARY KEY)")
        .await;
    protocol
        .ok(&owner, "CREATE TABLE b (id BIGINT PRIMARY KEY)")
        .await;
    protocol.ok(&owner, "DROP TABLE a, public.b").await;
    assert!(protocol.strings(&owner, CATALOG).await.is_empty());
    drop(owner);
    f.pg.shutdown().await.unwrap();
}

async fn ownership(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol
        .ok(&owner, "CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .await;
    protocol
        .ok(&owner, "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'")
        .await;
    protocol
        .ok(&owner, "GRANT CREATE ON SCHEMA public TO alice")
        .await;
    let alice = f.connect("alice", "alice-pw").await;
    protocol
        .ok(
            &alice,
            "CREATE TABLE mine (id BIGINT PRIMARY KEY, body TEXT)",
        )
        .await;
    protocol
        .ok(&alice, "INSERT INTO mine VALUES (1, 'alice-row')")
        .await;
    protocol.error(&alice, "DROP TABLE items", "42501").await;
    protocol
        .error(&alice, "DROP TABLE mine, items", "42501")
        .await;
    assert_eq!(
        protocol
            .strings(&alice, "SELECT body FROM mine LIMIT 1")
            .await,
        ["alice-row"]
    );
    protocol
        .error(&alice, "DROP TABLE IF EXISTS missing, items", "42501")
        .await;
    protocol.error(&owner, "DROP ROLE alice", "2BP01").await;
    protocol.ok(&alice, "DROP TABLE mine").await;
    assert!(protocol.strings(&alice, CATALOG).await.is_empty());
    protocol.ok(&owner, "DROP ROLE alice").await;
    assert_eq!(protocol.strings(&owner, CATALOG).await, ["items"]);
    drop(alice);
    drop(owner);
    f.pg.shutdown().await.unwrap();
}

async fn conflict(protocol: Protocol) {
    let f = Fixture::new().await;
    let a = f.connect("owner", "owner-pw").await;
    let b = f.connect("owner", "owner-pw").await;
    protocol
        .ok(&a, "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)")
        .await;
    protocol
        .ok(&a, "INSERT INTO t VALUES (1, 'snapshot-row')")
        .await;
    protocol.ok(&a, "BEGIN").await;
    assert_eq!(
        protocol.strings(&a, "SELECT body FROM t LIMIT 1").await,
        ["snapshot-row"]
    );
    protocol.error(&b, "DROP TABLE t", "55P03").await;
    // A failed drop must leave the active snapshot and committed table intact.
    assert_eq!(
        protocol.strings(&a, "SELECT body FROM t LIMIT 1").await,
        ["snapshot-row"]
    );
    protocol.ok(&a, "COMMIT").await;
    protocol.ok(&b, "DROP TABLE t").await;
    protocol
        .error(&b, "SELECT body FROM t LIMIT 1", "42P01")
        .await;
    drop(a);
    drop(b);
    f.pg.shutdown().await.unwrap();
}

async fn guards(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol
        .ok(&owner, "CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await;
    protocol
        .error(&owner, "DROP TABLE t CASCADE", "0A000")
        .await;
    protocol
        .error(&owner, "DROP TABLE t RESTRICT", "0A000")
        .await;
    protocol.ok(&owner, "BEGIN").await;
    protocol.error(&owner, "DROP TABLE t", "25001").await;
    protocol.ok(&owner, "ROLLBACK").await;
    assert_eq!(protocol.strings(&owner, CATALOG).await, ["t"]);
    protocol.ok(&owner, "DROP TABLE t").await;
    drop(owner);
    f.pg.shutdown().await.unwrap();
}

async fn application(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol
        .ok(&owner, "CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .await;
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
    protocol.error(&app, "DROP TABLE items", "42501").await;
    protocol
        .error(&app, "DROP TABLE IF EXISTS missing", "42501")
        .await;
    assert_eq!(protocol.strings(&owner, CATALOG).await, ["items"]);
    drop(app);
    drop(owner);
    agent.shutdown().await.unwrap();
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn simple_drop_and_recreate() {
    lifecycle(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_drop_and_recreate() {
    lifecycle(Protocol::Extended).await;
}
#[tokio::test]
async fn simple_validate_all_names() {
    validation(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_validate_all_names() {
    validation(Protocol::Extended).await;
}
#[tokio::test]
async fn simple_table_ownership() {
    ownership(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_table_ownership() {
    ownership(Protocol::Extended).await;
}
#[tokio::test]
async fn simple_transaction_conflict() {
    conflict(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_transaction_conflict() {
    conflict(Protocol::Extended).await;
}
#[tokio::test]
async fn simple_unsupported_and_transaction_guards() {
    guards(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_unsupported_and_transaction_guards() {
    guards(Protocol::Extended).await;
}
#[tokio::test]
async fn simple_application_cannot_drop() {
    application(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_application_cannot_drop() {
    application(Protocol::Extended).await;
}

#[tokio::test]
async fn mixed_case_unquoted_and_quoted_drop_targets() {
    for protocol in [Protocol::Simple, Protocol::Extended] {
        let f = Fixture::new().await;
        let owner = f.connect("owner", "owner-pw").await;
        for unquoted_drop in ["DROP TABLE MixedCase", "DROP TABLE mixedcase"] {
            protocol
                .ok(&owner, "CREATE TABLE MixedCase (id BIGINT PRIMARY KEY)")
                .await;
            assert_eq!(protocol.strings(&owner, CATALOG).await, ["mixedcase"]);
            protocol.error(&owner, "DROP TABLE \"MixedCase\"", "42P01").await;
            assert_eq!(protocol.strings(&owner, CATALOG).await, ["mixedcase"]);
            protocol.ok(&owner, unquoted_drop).await;
            assert!(protocol.strings(&owner, CATALOG).await.is_empty());
        }
        drop(owner);
        f.pg.shutdown().await.unwrap();
    }
}
