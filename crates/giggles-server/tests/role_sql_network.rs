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
    collections::BTreeMap,
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
    async fn integers(self, client: &Client, sql: &str) -> Vec<i32> {
        match self {
            Self::Simple => client
                .simple_query(sql)
                .await
                .unwrap_or_else(|e| panic!("{self:?}: {sql}: {e}"))
                .into_iter()
                .filter_map(|message| match message {
                    SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().parse().unwrap()),
                    _ => None,
                })
                .collect(),
            Self::Extended => client
                .query(sql, &[])
                .await
                .unwrap_or_else(|e| panic!("{self:?}: {sql}: {e}"))
                .into_iter()
                .map(|row| row.get(0))
                .collect(),
        }
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
        Self::with_tables(&["items"]).await
    }
    async fn with_tables(tables: &[&str]) -> Self {
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
                    tables: tables.iter().map(|table| (*table).to_owned()).collect(),
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
    async fn connect_application(&self) -> (Running, Client) {
        let server_tls = tls::server(
            vec![self.server.cert.der().clone()],
            PrivatePkcs8KeyDer::from(self.server.key.serialize_der()).into(),
            vec![self.ca.cert.der().clone()],
        )
        .unwrap();
        let agent = serve_pg(
            "127.0.0.1:0".parse().unwrap(),
            self.service.clone(),
            server_tls,
            8,
        )
        .await
        .unwrap();
        let client_tls = tls::client(
            vec![self.app.cert.der().clone()],
            PrivatePkcs8KeyDer::from(self.app.key.serialize_der()).into(),
            vec![self.ca.cert.der().clone()],
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
        (agent, app)
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

// The saved PG18.6 qualification supplies each minimal initial grant.
#[tokio::test]
async fn mutation_column_privileges_match_pg18_simple_and_extended() {
    for protocol in [Protocol::Simple, Protocol::Extended] {
        let f = Fixture::new().await;
        let owner = f.connect("owner", "owner-pw").await;
        protocol.ok(&owner, "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'").await;
        protocol.ok(&owner, "CREATE TABLE app(id BIGINT PRIMARY KEY, body BIGINT)").await;
        protocol.ok(&owner, "INSERT INTO app VALUES(1,1)").await;
        let alice = f.connect("alice", "alice-pw").await;
        for (statement, initial, added) in [
            ("UPDATE app SET body=1", "UPDATE", ""),
            ("UPDATE app SET body=1+1", "UPDATE", ""),
            ("UPDATE app SET body=1 WHERE true", "UPDATE", ""),
            ("UPDATE app SET body=1 WHERE id=1", "UPDATE", "SELECT"),
            ("UPDATE app SET body=body+1", "UPDATE", "SELECT"),
            ("DELETE FROM app WHERE true", "DELETE", ""),
            ("DELETE FROM app WHERE id=1", "DELETE", "SELECT"),
            ("INSERT INTO app VALUES (2,1) RETURNING 1", "INSERT", ""),
            ("UPDATE app SET body=1 RETURNING 1", "UPDATE", ""),
            ("DELETE FROM app RETURNING 1", "DELETE", ""),
            ("INSERT INTO app VALUES (2,1) RETURNING id", "INSERT", "SELECT"),
            ("UPDATE app SET body=1 RETURNING body", "UPDATE", "SELECT"),
            ("DELETE FROM app RETURNING id", "DELETE", "SELECT"),
            ("INSERT INTO app VALUES (1,1) ON CONFLICT(id) DO NOTHING", "INSERT", "SELECT"),
            ("INSERT INTO app VALUES (1,1) ON CONFLICT DO NOTHING", "INSERT", ""),
            ("INSERT INTO app VALUES (1,1) ON CONFLICT ON CONSTRAINT app_pkey DO NOTHING", "INSERT", "SELECT"),
            ("INSERT INTO app VALUES (1,1) ON CONFLICT(id) DO UPDATE SET body=1", "INSERT,SELECT", "UPDATE"),
            ("INSERT INTO app VALUES (1,1) ON CONFLICT(id) DO UPDATE SET body=1", "INSERT,UPDATE", "SELECT"),
            ("INSERT INTO app VALUES (1,1) ON CONFLICT ON CONSTRAINT app_pkey DO UPDATE SET body=app.body+1", "INSERT,UPDATE", "SELECT"),
            ("INSERT INTO app VALUES (1,1) ON CONFLICT ON CONSTRAINT app_pkey DO UPDATE SET body=excluded.body", "INSERT,UPDATE", "SELECT"),
            ("INSERT INTO app VALUES (1,1) ON CONFLICT ON CONSTRAINT app_pkey DO UPDATE SET body=1 WHERE app.body=1", "INSERT,UPDATE", "SELECT"),
            ("INSERT INTO app VALUES (1,1) ON CONFLICT ON CONSTRAINT app_pkey DO UPDATE SET body=1 WHERE excluded.body=1", "INSERT,UPDATE", "SELECT"),
            ("INSERT INTO app VALUES (1,1) ON CONFLICT ON CONSTRAINT app_pkey DO UPDATE SET body=1", "INSERT,UPDATE", "SELECT"),
        ] {
            protocol.ok(&owner, "REVOKE SELECT,INSERT,UPDATE,DELETE ON app FROM alice").await;
            protocol.ok(&owner, &format!("GRANT {initial} ON app TO alice")).await;
            if !added.is_empty() {
                protocol.error(&alice, statement, "42501").await;
                protocol.ok(&owner, &format!("GRANT {added} ON app TO alice")).await;
            }
            protocol.ok(&alice, "BEGIN").await;
            protocol.ok(&alice, statement).await;
            protocol.ok(&alice, "ROLLBACK").await;
        }
        drop(alice);
        drop(owner);
        f.pg.shutdown().await.unwrap();
    }
}

// Verified against PostgreSQL 18.6 with an unprivileged role and an owner-only table.
#[tokio::test]
async fn relation_free_plans_need_no_table_privileges_simple_and_extended() {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    owner
        .simple_query("CREATE ROLE alice LOGIN PASSWORD 'alice-pw'")
        .await
        .unwrap();
    owner
        .simple_query("CREATE TABLE t (id INT)")
        .await
        .unwrap();
    let alice = f.connect("alice", "alice-pw").await;
    let cases: [(&str, &[i32]); 3] = [
        ("VALUES (1)", &[1]),
        ("SELECT 1 UNION SELECT 2", &[1, 2]),
        ("WITH q AS (SELECT 1 AS a) SELECT a FROM q", &[1]),
    ];
    for protocol in [Protocol::Simple, Protocol::Extended] {
        for (sql, expected) in cases {
            let mut actual: Vec<i32> = match protocol {
                Protocol::Simple => alice
                    .simple_query(sql)
                    .await
                    .unwrap()
                    .into_iter()
                    .filter_map(|message| match message {
                        SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().parse().unwrap()),
                        _ => None,
                    })
                    .collect(),
                Protocol::Extended => alice
                    .query(sql, &[])
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|row| row.get(0))
                    .collect(),
            };
            actual.sort_unstable();
            assert_eq!(actual, expected, "{protocol:?}: {sql}");
        }
        protocol
            .error(&alice, "SELECT id FROM t UNION SELECT 1", "42501")
            .await;
    }
    // Application bindings use the same planner authorization branch.
    let mut app = f
        .service
        .session(&tls::fingerprint(f.app.cert.der()))
        .unwrap();
    for (sql, expected) in cases {
        let result = app.execute_sql(sql, &[]).unwrap();
        let mut actual: Vec<_> = result
            .rows
            .into_iter()
            .map(|row| match row.values.as_slice() {
                [giggles_core::relational::Value::Int(value)] => *value,
                values => panic!("application: {sql}: unexpected row {values:?}"),
            })
            .collect();
        actual.sort_unstable();
        let expected: Vec<_> = expected.iter().map(|value| i64::from(*value)).collect();
        assert_eq!(actual, expected, "application: {sql}");
    }
    let error = app
        .execute_sql("SELECT id FROM t UNION SELECT 1", &[])
        .unwrap_err();
    assert_eq!(giggles_server::sqlstate(&error), "42501");
    drop(app);
    drop(alice);
    drop(owner);
    f.pg.shutdown().await.unwrap();
}

// PostgreSQL 18.6: INNER/LEFT joins return 1 with both SELECT grants and 42501
// without the second grant, using simple queries and psql's extended \bind.
#[tokio::test]
async fn existing_path_joins_require_every_table_simple_and_extended() {
    let joins = [
        "SELECT a.id FROM a JOIN b ON a.id = b.id",
        "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id",
    ];
    for both_tables in [true, false] {
        let tables: &[&str] = if both_tables { &["a", "b"] } else { &["a"] };
        let f = Fixture::with_tables(tables).await;
        let owner = f.connect("owner", "owner-pw").await;
        for sql in [
            "CREATE TABLE a (id INT)",
            "CREATE TABLE b (id INT)",
            "INSERT INTO a VALUES (1)",
            "INSERT INTO b VALUES (1)",
            "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'",
            "GRANT SELECT ON a TO alice",
        ] {
            owner.simple_query(sql).await.unwrap();
        }
        if both_tables {
            owner
                .simple_query("GRANT SELECT ON b TO alice")
                .await
                .unwrap();
        }
        let alice = f.connect("alice", "alice-pw").await;
        let (agent, app) = f.connect_application().await;
        for protocol in [Protocol::Simple, Protocol::Extended] {
            for (role, client) in [("database", &alice), ("application", &app)] {
                for sql in joins {
                    if !both_tables {
                        protocol.error(client, sql, "42501").await;
                        continue;
                    }
                    let actual: Vec<i32> = match protocol {
                        Protocol::Simple => client
                            .simple_query(sql)
                            .await
                            .unwrap()
                            .into_iter()
                            .filter_map(|message| match message {
                                SimpleQueryMessage::Row(row) => {
                                    Some(row.get(0).unwrap().parse().unwrap())
                                }
                                _ => None,
                            })
                            .collect(),
                        Protocol::Extended => client
                            .query(sql, &[])
                            .await
                            .unwrap()
                            .into_iter()
                            .map(|row| row.get(0))
                            .collect(),
                    };
                    assert_eq!(actual, [1], "{role}: {protocol:?}: {sql}");
                }
            }
        }
        drop(app);
        drop(alice);
        drop(owner);
        agent.shutdown().await.unwrap();
        f.pg.shutdown().await.unwrap();
    }
}

// PostgreSQL 18.6, simple and extended: WHERE/RETURNING columns need SELECT;
// constant RETURNING needs only DELETE. The planner currently requires a SELECT
// main statement, so the INSERT is a second CTE (whose RETURNING also needs SELECT).
#[tokio::test]
async fn modifying_with_requires_read_and_write_privileges_simple_and_extended() {
    let delete = "WITH d AS (DELETE FROM t WHERE id = 1 RETURNING id) SELECT id FROM d";
    let constant = "WITH d AS (DELETE FROM t RETURNING 1 AS id) SELECT id FROM d";
    let transfer = "WITH d AS (DELETE FROM t WHERE id = 2 RETURNING id), a AS (INSERT INTO t2 SELECT id FROM d RETURNING id) SELECT id FROM a";
    for protocol in [Protocol::Simple, Protocol::Extended] {
        let f = Fixture::new().await;
        let owner = f.connect("owner", "owner-pw").await;
        for sql in [
            "CREATE TABLE t (id INT)",
            "CREATE TABLE t2 (id INT)",
            "INSERT INTO t VALUES (1), (2)",
            "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'",
        ] {
            protocol.ok(&owner, sql).await;
        }
        let alice = f.connect("alice", "alice-pw").await;
        protocol.error(&alice, constant, "42501").await;
        protocol.ok(&owner, "GRANT DELETE ON t TO alice").await;
        protocol.ok(&alice, "BEGIN").await;
        assert_eq!(protocol.integers(&alice, constant).await, [1, 1]);
        protocol.ok(&alice, "ROLLBACK").await;
        protocol.error(&alice, delete, "42501").await;
        protocol.ok(&owner, "GRANT SELECT ON t TO alice").await;
        protocol.ok(&alice, "BEGIN").await;
        assert_eq!(protocol.integers(&alice, delete).await, [1]);
        protocol.ok(&alice, "ROLLBACK").await;
        protocol.ok(&owner, "REVOKE DELETE ON t FROM alice").await;
        protocol.error(&alice, delete, "42501").await;
        protocol.ok(&owner, "GRANT DELETE ON t TO alice").await;
        protocol.ok(&owner, "GRANT INSERT ON t2 TO alice").await;
        protocol.error(&alice, transfer, "42501").await;
        protocol.ok(&owner, "GRANT SELECT ON t2 TO alice").await;
        protocol.ok(&alice, "BEGIN").await;
        assert_eq!(protocol.integers(&alice, transfer).await, [2]);
        protocol.ok(&alice, "ROLLBACK").await;
        protocol.ok(&owner, "REVOKE INSERT ON t2 FROM alice").await;
        protocol.error(&alice, transfer, "42501").await;
        assert_eq!(
            protocol.integers(&owner, "SELECT id FROM t ORDER BY id").await,
            [1, 2]
        );
        assert!(protocol.integers(&owner, "SELECT id FROM t2").await.is_empty());
        drop(alice);
        drop(owner);
        f.pg.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn modifying_with_application_scope_covers_all_tables_simple_and_extended() {
    let delete = "WITH d AS (DELETE FROM t WHERE id = 1 RETURNING id) SELECT id FROM d";
    let constant = "WITH d AS (DELETE FROM t RETURNING 1 AS id) SELECT id FROM d";
    let transfer = "WITH d AS (DELETE FROM t WHERE id = 2 RETURNING id), a AS (INSERT INTO t2 SELECT id FROM d RETURNING id) SELECT id FROM a";
    for tables in [&["outside"][..], &["t"][..], &["t2"][..], &["t", "t2"][..]] {
        let f = Fixture::with_tables(tables).await;
        let owner = f.connect("owner", "owner-pw").await;
        for sql in [
            "CREATE TABLE t (id INT)",
            "CREATE TABLE t2 (id INT)",
            "INSERT INTO t VALUES (1), (2)",
        ] {
            owner.simple_query(sql).await.unwrap();
        }
        let (agent, app) = f.connect_application().await;
        for protocol in [Protocol::Simple, Protocol::Extended] {
            for (sql, expected, allowed) in [
                (delete, &[1][..], tables.contains(&"t")),
                (constant, &[1, 1][..], tables.contains(&"t")),
                (
                    transfer,
                    &[2][..],
                    tables.contains(&"t") && tables.contains(&"t2"),
                ),
            ] {
                if allowed {
                    protocol.ok(&app, "BEGIN").await;
                    assert_eq!(
                        protocol.integers(&app, sql).await,
                        expected,
                        "{tables:?}: {protocol:?}: {sql}"
                    );
                    protocol.ok(&app, "ROLLBACK").await;
                } else {
                    protocol.error(&app, sql, "42501").await;
                }
            }
        }
        assert_eq!(
            Protocol::Simple.integers(&owner, "SELECT id FROM t ORDER BY id").await,
            [1, 2]
        );
        assert!(
            Protocol::Simple.integers(&owner, "SELECT id FROM t2").await.is_empty()
        );
        drop(app);
        drop(owner);
        agent.shutdown().await.unwrap();
        f.pg.shutdown().await.unwrap();
    }
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
    protocol.ok(&alice, "BEGIN").await;
    protocol.ok(&alice, "ROLLBACK").await;
    f.bad_password("alice", "alice-pw").await;
    let alice = f.connect("alice", "new-pw").await;
    protocol
        .error(&alice, "ALTER ROLE alice SUPERUSER", "42501")
        .await;
    protocol.error(&alice, "CREATE ROLE bob", "42501").await;
    protocol
        .ok(&alice, "ALTER ROLE alice PASSWORD 'self-pw'")
        .await;
    protocol.ok(&alice, "BEGIN").await;
    protocol.ok(&alice, "ROLLBACK").await;
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
    let (agent, app) = f.connect_application().await;
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
