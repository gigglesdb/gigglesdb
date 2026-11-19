use giggles_core::{
    Admin, Limits,
    roles::{NewRole, SchemaPrivilege, TablePrivilege},
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
            "sequence-authority-network-{}-{}-{}",
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
        for name in ["alice", "outsider"] {
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

// Owning the owning table is GigglesDB's analogue of PostgreSQL sequence
// USAGE/UPDATE privileges. SELECT/INSERT grants alone are insufficient.
async fn authority(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol
        .ok(
            &owner,
            "CREATE TABLE items (id serial PRIMARY KEY, body text)",
        )
        .await;
    protocol
        .ok(&owner, "CREATE TABLE secret (id serial PRIMARY KEY)")
        .await;
    f.admin.roles().set_table_owner("items", "alice").unwrap();
    f.admin
        .roles()
        .grant_table(
            "items",
            "outsider",
            &[
                TablePrivilege::Select,
                TablePrivilege::Insert,
                TablePrivilege::Update,
            ],
        )
        .unwrap();
    let alice = f.connect("alice", "pw").await;
    let outsider = f.connect("outsider", "pw").await;
    for function in ["nextval", "currval", "setval"] {
        for sequence in ["items_id_seq", "absent_seq"] {
            let args = if function == "setval" { ", 7" } else { "" };
            let sql = format!("SELECT {function}('{sequence}'{args})");
            protocol.error(&outsider, &sql, "42501").await;
            if sequence == "absent_seq" {
                protocol.error(&alice, &sql, "42P01").await;
                protocol.error(&owner, &sql, "42P01").await;
            }
        }
    }
    protocol
        .error(&alice, "SELECT currval('items_id_seq')", "55000")
        .await;
    protocol
        .ok(&alice, "SELECT nextval('items_id_seq'::regclass)")
        .await;
    protocol.ok(&alice, "SELECT currval('items_id_seq')").await;
    protocol
        .ok(&alice, "SELECT setval('items_id_seq', 9, false)")
        .await;
    protocol.ok(&alice, "SELECT nextval('items_id_seq')").await;
    protocol
        .error(&alice, "SELECT nextval('public.secret_id_seq')", "42501")
        .await;
    protocol
        .error(
            &outsider,
            "INSERT INTO items (body) VALUES ('denied')",
            "42501",
        )
        .await;
    protocol
        .ok(
            &outsider,
            "INSERT INTO items (id, body) VALUES (100, 'explicit')",
        )
        .await;
    // Even an empty match set requires authority for the requested DEFAULT.
    protocol
        .error(
            &outsider,
            "UPDATE items SET id=DEFAULT WHERE id=999",
            "42501",
        )
        .await;
    protocol
        .error(
            &outsider,
            "UPDATE absent_table SET id=DEFAULT WHERE id=999",
            "42501",
        )
        .await;
    f.admin
        .roles()
        .grant_schema("public", "outsider", &[SchemaPrivilege::Create])
        .unwrap();
    for sequence in ["items_id_seq", "absent_seq"] {
        protocol
            .error(
                &outsider,
                &format!("CREATE TABLE probe (id int DEFAULT nextval('{sequence}'::regclass))"),
                "42501",
            )
            .await;
    }

    if matches!(protocol, Protocol::Extended) {
        let dynamic = alice.prepare("SELECT nextval($1)").await.unwrap();
        let row = alice.query_one(&dynamic, &[&"items_id_seq"]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), 10);
        let error = alice
            .query_one(&dynamic, &[&"secret_id_seq"])
            .await
            .unwrap_err();
        assert_eq!(error.code().unwrap().code(), "42501");
        let error = alice
            .query_one(&dynamic, &[&"absent_seq"])
            .await
            .unwrap_err();
        assert_eq!(error.code().unwrap().code(), "42P01");
        let prepared = alice
            .prepare("SELECT nextval('items_id_seq')")
            .await
            .unwrap();
        f.admin
            .roles()
            .set_table_owner("items", "outsider")
            .unwrap();
        let error = alice.query_one(&prepared, &[]).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "42501");
        f.admin.roles().set_table_owner("items", "alice").unwrap();
    }
    // Denied calls and prepared revocation must not consume sequence values.
    let row = owner
        .query_one("SELECT nextval('secret_id_seq')", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);

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
    protocol.ok(&app, "SELECT nextval('items_id_seq')").await;
    protocol
        .error(&app, "SELECT nextval('secret_id_seq')", "42501")
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
    drop(task);
    drop(outsider);
    drop(alice);
    drop(owner);
    agent.shutdown().await.unwrap();
    f.pg.shutdown().await.unwrap();
}
#[tokio::test]
async fn simple_sequence_authority_precedes_resolution() {
    authority(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_sequence_authority_precedes_resolution() {
    authority(Protocol::Extended).await;
}
