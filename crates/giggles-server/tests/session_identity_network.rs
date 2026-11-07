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
            "session-identity-network-{}-{}-{}",
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
                name: "alice".into(),
                login: true,
                superuser: true,
                password: Some(Verifier::generate("alice-pw")),
                ..NewRole::default()
            })
            .unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "bob".into(),
                login: true,
                password: Some(Verifier::generate("bob-pw")),
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
            databases: vec!["appdb".into(), "postgres".into()],
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
    async fn connect(&self, user: &str, database: &str) -> Client {
        let mut config = Config::new();
        config
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
            .password(format!("{user}-pw"))
            .dbname(database)
            .ssl_mode(SslMode::Disable);
        let (client, connection) = config.connect(NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
}

const IDENTITY: &str =
    "current_user, session_user, current_role, user, current_database(), current_catalog";

async fn assert_identity(client: &Client, user: &str, database: &str) {
    let expected = [user, user, user, user, database, database];
    let forms = [
        "current_user",
        "session_user",
        "current_role",
        "user",
        "current_database()",
        "current_catalog",
    ];
    for (form, value) in forms.iter().zip(expected) {
        assert_strings(client, &format!("SELECT {form}"), &[value]).await;
    }
    assert_strings(client, &format!("SELECT {IDENTITY}"), &expected).await;
    // Exercise expressions in a catalog projection and predicate as well as table-free SELECTs.
    assert_strings(client, &format!("SELECT {IDENTITY} FROM pg_catalog.pg_database WHERE current_user = session_user AND user = current_role AND current_database() = current_catalog LIMIT 1"), &expected).await;
}

async fn assert_strings(client: &Client, sql: &str, expected: &[&str]) {
    let simple = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("simple {sql}: {e}"));
    let rows = simple
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 1, "simple {sql}");
    let values = (0..rows[0].len())
        .map(|i| rows[0].get(i).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(values, expected, "simple {sql}");
    let rows = client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("extended {sql}: {e}"));
    assert_eq!(rows.len(), 1, "extended {sql}");
    let values = (0..rows[0].len())
        .map(|i| rows[0].get::<_, String>(i))
        .collect::<Vec<_>>();
    assert_eq!(values, expected, "extended {sql}");
}

#[tokio::test]
async fn postgresql_identity_uses_authenticated_role_and_startup_database() {
    let f = Fixture::new().await;
    let alice = f.connect("alice", "appdb").await;
    assert_identity(&alice, "alice", "appdb").await;
    let bob = f.connect("bob", "postgres").await;
    assert_identity(&bob, "bob", "postgres").await;
    // A second connection must not replace the first session's identity.
    assert_identity(&alice, "alice", "appdb").await;
    drop(alice);
    drop(bob);
    f.pg.shutdown().await.unwrap();
}

#[tokio::test]
async fn agent_application_identity_remains_giggles() {
    let f = Fixture::new().await;
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
    let (client, connection) = config
        .connect(MakeRustlsConnect::new((*client_tls).clone()))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    assert_identity(&client, "giggles", "giggles").await;
    drop(client);
    agent.shutdown().await.unwrap();
    f.pg.shutdown().await.unwrap();
}
