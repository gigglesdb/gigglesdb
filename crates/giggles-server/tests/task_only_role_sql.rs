//! R3-109 (GD-0061): CREATE ROLE / ALTER ROLE ... TASKONLY over a real pgwire client.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
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
use tokio_postgres::{Client, Config, NoTls, config::SslMode};

static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    pg: Running,
    admin: Admin,
    _service: Arc<Service>,
}
impl Fixture {
    async fn new(engine: Engine) -> Self {
        let root = std::path::PathBuf::from(
            std::env::var_os("TEMP").expect("set task TEMP under J:/gigglesdb-work"),
        );
        let path = root.join(format!(
            "task-only-role-sql-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap();
        for (name, superuser, createrole) in [
            ("owner", true, false),
            ("manager", false, true),
            ("plain", false, false),
        ] {
            admin
                .roles()
                .create(NewRole {
                    inherit: None,
                    name: name.into(),
                    login: true,
                    superuser,
                    createrole,
                    password: Some(Verifier::generate(&format!("{name}-pw"))),
                })
                .unwrap();
        }
        let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
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
        let pg = serve_postgresql(config.listen, service.clone(), Arc::new(listener))
            .await
            .unwrap();
        Self {
            pg,
            admin,
            _service: service,
        }
    }
    async fn connect(&self, user: &str) -> Client {
        let mut config = Config::new();
        config
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
            .password(format!("{user}-pw"))
            .dbname("postgres")
            .ssl_mode(SslMode::Disable);
        let (client, connection) = config.connect(NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    fn task_only(&self, name: &str) -> bool {
        self.admin.roles().get(name).unwrap().unwrap().task_only
    }
}

async fn ok(client: &Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}
async fn error(client: &Client, sql: &str, expected: &str) {
    let e = client.simple_query(sql).await.unwrap_err();
    assert_eq!(e.code().expect("SQLSTATE").code(), expected, "{sql}: {e}");
}

#[tokio::test]
async fn create_taskonly_role() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    ok(&owner, "CREATE ROLE repair_agent LOGIN PASSWORD 'agent-pw' TASKONLY").await;
    assert!(f.task_only("repair_agent"));
    let manager = f.connect("manager").await;
    ok(&manager, "CREATE USER helper_agent WITH TASKONLY").await;
    assert!(f.task_only("helper_agent"));
    ok(&owner, "CREATE ROLE ordinary LOGIN").await;
    assert!(!f.task_only("ordinary"));
    error(&owner, "CREATE ROLE twice TASKONLY TASKONLY", "42601").await;
}

#[tokio::test]
async fn taskonly_with_superuser_22023() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    error(&owner, "CREATE ROLE a SUPERUSER TASKONLY", "22023").await;
    error(&owner, "CREATE ROLE a CREATEROLE TASKONLY", "22023").await;
    assert!(f.admin.roles().get("a").unwrap().is_none());
    ok(&owner, "CREATE ROLE b LOGIN").await;
    error(&owner, "ALTER ROLE b SUPERUSER TASKONLY", "22023").await;
    assert!(!f.task_only("b"));
}

#[tokio::test]
async fn alter_owner_role_to_taskonly_42p17() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    ok(&owner, "CREATE ROLE builder LOGIN PASSWORD 'builder-pw'").await;
    ok(&owner, "GRANT CREATE ON SCHEMA public TO builder").await;
    let builder = f.connect("builder").await;
    ok(&builder, "CREATE TABLE built (id BIGINT PRIMARY KEY)").await;
    // builder owns a table: D6 forbids making it task-only (section 7: 42P17).
    error(&owner, "ALTER ROLE builder TASKONLY", "42P17").await;
    assert!(!f.task_only("builder"));
    // A superuser cannot become task-only either.
    error(&owner, "ALTER ROLE owner TASKONLY", "42P17").await;
}

#[tokio::test]
async fn alter_notaskonly() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    ok(&owner, "CREATE ROLE agent LOGIN").await;
    ok(&owner, "ALTER ROLE agent TASKONLY").await;
    assert!(f.task_only("agent"));
    // A task-only role cannot gain SUPERUSER or CREATEROLE.
    error(&owner, "ALTER ROLE agent SUPERUSER", "42P17").await;
    error(&owner, "ALTER ROLE agent CREATEROLE", "42P17").await;
    ok(&owner, "ALTER ROLE agent NOTASKONLY").await;
    assert!(!f.task_only("agent"));
    ok(&owner, "ALTER ROLE agent CREATEROLE").await;
    // One statement may drop CREATEROLE and set TASKONLY, or the reverse.
    ok(&owner, "ALTER ROLE agent NOCREATEROLE TASKONLY").await;
    assert!(f.task_only("agent"));
    ok(&owner, "ALTER ROLE agent NOTASKONLY CREATEROLE").await;
    let role = f.admin.roles().get("agent").unwrap().unwrap();
    assert!(!role.task_only && role.createrole);
}

#[tokio::test]
async fn non_admin_42501() {
    let f = Fixture::new(Engine::V2).await;
    let owner = f.connect("owner").await;
    ok(&owner, "CREATE ROLE target LOGIN").await;
    let plain = f.connect("plain").await;
    error(&plain, "CREATE ROLE x TASKONLY", "42501").await;
    error(&plain, "ALTER ROLE target TASKONLY", "42501").await;
    // Changing one's own task-only attribute is not a password change.
    error(&plain, "ALTER ROLE plain TASKONLY", "42501").await;
    assert!(!f.task_only("target"));
}

#[tokio::test]
async fn legacy_server_0a000() {
    let f = Fixture::new(Engine::Legacy).await;
    let owner = f.connect("owner").await;
    error(&owner, "CREATE ROLE agent LOGIN TASKONLY", "0A000").await;
    // The half-created role is dropped again.
    assert!(f.admin.roles().get("agent").unwrap().is_none());
    ok(&owner, "CREATE ROLE agent LOGIN").await;
    error(&owner, "ALTER ROLE agent TASKONLY", "0A000").await;
}
