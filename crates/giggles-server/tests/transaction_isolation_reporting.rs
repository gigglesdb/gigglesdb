use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
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

struct Fixture {
    service: Arc<Service>,
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new(engine: Engine) -> Self {
        let path = std::env::temp_dir().join(format!(
            "isolation-reporting-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db"), Limits::default(), engine).unwrap();
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
async fn show(client: &Client, query: &str) -> Vec<String> {
    values(&client.simple_query(query).await.unwrap())
}
/// GD-0075 R5-75: every isolation setting reports the level the engine runs,
/// whatever was requested (PostgreSQL 18.6 reports the requested level, which
/// the engine must then honour; R5-76).
async fn reports_the_level_the_engine_runs(engine: Engine, runs: &str) {
    let fixture = Fixture::new(engine).await;
    let client = fixture.client().await;
    const TRANSACTION: [&str; 4] = [
        "SHOW transaction_isolation",
        "SHOW TRANSACTION ISOLATION LEVEL",
        "SELECT current_setting('transaction_isolation')",
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'transaction_isolation'",
    ];
    const DEFAULT: [&str; 3] = [
        "SHOW default_transaction_isolation",
        "SELECT current_setting('default_transaction_isolation')",
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'default_transaction_isolation'",
    ];
    for query in TRANSACTION.into_iter().chain(DEFAULT) {
        assert_eq!(show(&client, query).await, vec![runs], "{engine:?} default: {query}");
    }
    for level in [
        "read uncommitted",
        "read committed",
        "repeatable read",
        "serializable",
    ] {
        for begin in [true, false] {
            let sql = if begin {
                format!("BEGIN ISOLATION LEVEL {level}")
            } else {
                format!("BEGIN; SET TRANSACTION ISOLATION LEVEL {level}")
            };
            client.batch_execute(&sql).await.unwrap();
            for query in TRANSACTION.into_iter().chain(DEFAULT) {
                assert_eq!(show(&client, query).await, vec![runs], "{engine:?} {sql}: {query}");
            }
            client.batch_execute("ROLLBACK").await.unwrap();
            assert_eq!(show(&client, "SHOW transaction_isolation").await, vec![runs]);
        }
        for set in [
            format!("SET default_transaction_isolation = '{level}'"),
            format!("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}"),
        ] {
            client.batch_execute(&set).await.unwrap();
            for query in TRANSACTION.into_iter().chain(DEFAULT) {
                assert_eq!(show(&client, query).await, vec![runs], "{engine:?} {set}: {query}");
            }
            client.batch_execute("BEGIN").await.unwrap();
            assert_eq!(show(&client, "SHOW transaction_isolation").await, vec![runs], "{set}");
            client.batch_execute("COMMIT").await.unwrap();
        }
        client
            .batch_execute("RESET default_transaction_isolation")
            .await
            .unwrap();
    }
    drop(client);
    fixture.stop().await;
}
#[tokio::test]
async fn legacy_reports_serializable_for_every_requested_level() {
    reports_the_level_the_engine_runs(Engine::Legacy, "serializable").await;
}
/// GD-0075 R5-76: v2 runs the requested level, so it reports what PostgreSQL
/// 18.6 reports (J:/gigglesdb-work/r5/e/oracle/report.out).
#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn v2_reports_the_requested_level_like_postgresql() {
    let fixture = Fixture::new(Engine::V2).await;
    let client = fixture.client().await;
    const TRANSACTION: [&str; 4] = [
        "SHOW transaction_isolation",
        "SHOW TRANSACTION ISOLATION LEVEL",
        "SELECT current_setting('transaction_isolation')",
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'transaction_isolation'",
    ];
    const DEFAULT: [&str; 3] = [
        "SHOW default_transaction_isolation",
        "SELECT current_setting('default_transaction_isolation')",
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'default_transaction_isolation'",
    ];
    async fn expect(client: &Client, transaction: &str, default: &str, context: &str) {
        for query in TRANSACTION {
            assert_eq!(show(client, query).await, vec![transaction], "{context}: {query}");
        }
        for query in DEFAULT {
            assert_eq!(show(client, query).await, vec![default], "{context}: {query}");
        }
    }
    expect(&client, "read committed", "read committed", "startup").await;
    for level in [
        "read uncommitted",
        "read committed",
        "repeatable read",
        "serializable",
    ] {
        for sql in [
            format!("BEGIN ISOLATION LEVEL {level}"),
            format!("BEGIN; SET TRANSACTION ISOLATION LEVEL {level}"),
        ] {
            client.batch_execute(&sql).await.unwrap();
            expect(&client, level, "read committed", &sql).await;
            client.batch_execute("ROLLBACK").await.unwrap();
            expect(&client, "read committed", "read committed", &sql).await;
        }
        for set in [
            format!("SET default_transaction_isolation = '{level}'"),
            format!("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}"),
        ] {
            client.batch_execute(&set).await.unwrap();
            expect(&client, level, level, &set).await;
            client.batch_execute("BEGIN").await.unwrap();
            expect(&client, level, level, &set).await;
            client.batch_execute("COMMIT").await.unwrap();
        }
        client
            .batch_execute("RESET default_transaction_isolation")
            .await
            .unwrap();
        expect(&client, "read committed", "read committed", "RESET").await;
    }
    // A new level after the first query is 25001, as in PostgreSQL.
    client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE; SELECT 1")
        .await
        .unwrap();
    let error = client
        .batch_execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "25001");
    client.batch_execute("ROLLBACK").await.unwrap();
    drop(client);
    fixture.stop().await;
}
