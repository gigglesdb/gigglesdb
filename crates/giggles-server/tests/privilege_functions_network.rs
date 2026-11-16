use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, config::SslMode};
static SERIAL: AtomicU64 = AtomicU64::new(0);
#[derive(Clone, Copy, Debug)]
enum Protocol {
    Simple,
    Extended,
}
impl Protocol {
    async fn ok(self, client: &Client, sql: &str) {
        match self {
            Self::Simple => {
                client
                    .simple_query(sql)
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            }
            Self::Extended => {
                client
                    .execute(sql, &[])
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            }
        }
    }
    async fn boolean(self, client: &Client, expression: &str, expected: bool) {
        let sql = format!("SELECT {expression}");
        let actual = match self {
            Self::Simple => client
                .simple_query(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
                .into_iter()
                .find_map(|r| {
                    if let SimpleQueryMessage::Row(r) = r {
                        Some(r.get(0).unwrap() == "t")
                    } else {
                        None
                    }
                })
                .unwrap(),
            Self::Extended => client
                .query_one(&sql, &[])
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
                .get(0),
        };
        assert_eq!(actual, expected, "{self:?}: {expression}");
    }
    async fn acl(self, client: &Client, expected: &[&str]) {
        let sql = "SELECT x.privilege_type FROM pg_catalog.pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) x WHERE c.relname='t' AND x.grantee=(SELECT oid FROM pg_roles WHERE rolname='app') ORDER BY x.privilege_type";
        let actual: Vec<String> = match self {
            Self::Simple => client
                .simple_query(sql)
                .await
                .unwrap()
                .into_iter()
                .filter_map(|r| {
                    if let SimpleQueryMessage::Row(r) = r {
                        Some(r.get(0).unwrap().to_owned())
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
        };
        assert_eq!(actual, expected, "{self:?}: real relacl grants");
    }
    async fn invalid_privilege(self, client: &Client) {
        let sql = "SELECT has_table_privilege('t','invalid')";
        let error = match self {
            Self::Simple => client.simple_query(sql).await.unwrap_err(),
            Self::Extended => client.query(sql, &[]).await.unwrap_err(),
        };
        assert_eq!(error.code().unwrap().code(), "22023");
    }
}
struct Fixture {
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new() -> Self {
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
            "privilege-functions-{}-{}-{}",
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
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let config = PostgresqlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: TlsMode::Disabled,
            certificate: None,
            private_key: None,
            client_ca: None,
            certificates: BTreeMap::new(),
            databases: vec!["postgres".into()],
            loopback_trust: false,
            connections: 16,
        };
        let listener = config.build().unwrap();
        let pg = serve_postgresql(config.listen, service, Arc::new(listener))
            .await
            .unwrap();
        Self { pg, path }
    }
    async fn connect(&self, user: &str, password: &str) -> Client {
        let mut config = Config::new();
        config
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
            .password(password)
            .dbname("postgres")
            .ssl_mode(SslMode::Disable);
        let (client, connection) = config.connect(NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    async fn finish(self) {
        self.pg.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}
async fn privileges(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol
        .ok(&owner, "CREATE ROLE app LOGIN PASSWORD 'app-pw'")
        .await;
    protocol
        .ok(&owner, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;
    protocol
        .ok(&owner, "GRANT SELECT, INSERT ON t TO app")
        .await;
    let app = f.connect("app", "app-pw").await;
    for (expression, expected) in [
        ("has_table_privilege('t','SELECT')", true),
        ("has_table_privilege('t','UPDATE')", false),
        ("has_table_privilege('t','INSERT')", true),
        ("has_table_privilege('app','public.t','DELETE')", false),
        ("has_column_privilege('t','id','SELECT')", true),
        ("has_column_privilege('t','id','UPDATE')", false),
    ] {
        protocol.boolean(&app, expression, expected).await;
    }
    protocol
        .boolean(&owner, "has_table_privilege('app','t','UPDATE')", false)
        .await;
    protocol.acl(&app, &["INSERT", "SELECT"]).await;
    protocol.invalid_privilege(&app).await;
    protocol.ok(&owner, "GRANT UPDATE ON t TO app").await;
    protocol
        .boolean(&app, "has_table_privilege('t','UPDATE')", true)
        .await;
    protocol
        .boolean(&owner, "has_table_privilege('app','t','UPDATE')", true)
        .await;
    protocol.acl(&app, &["INSERT", "SELECT", "UPDATE"]).await;
    protocol.ok(&owner, "REVOKE UPDATE ON t FROM app").await;
    protocol
        .boolean(&app, "has_table_privilege('t','UPDATE')", false)
        .await;
    protocol.acl(&app, &["INSERT", "SELECT"]).await;
    drop(app);
    drop(owner);
    f.finish().await;
}
#[tokio::test]
async fn simple_real_grants_and_revoke() {
    privileges(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_real_grants_and_revoke() {
    privileges(Protocol::Extended).await;
}
