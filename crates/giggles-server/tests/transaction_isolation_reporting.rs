use giggles_core::{Admin, Limits, roles::NewRole};
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
    async fn new() -> Self {
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
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
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
#[tokio::test]
async fn requested_transaction_isolation_is_reported_everywhere() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
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
            for query in [
                "SHOW transaction_isolation",
                "SELECT current_setting('transaction_isolation')",
                "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'transaction_isolation'",
            ] {
                assert_eq!(
                    values(&client.simple_query(query).await.unwrap()),
                    vec![level],
                    "{sql}: {query}"
                );
            }
            for query in [
                "SHOW default_transaction_isolation",
                "SELECT current_setting('default_transaction_isolation')",
                "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'default_transaction_isolation'",
            ] {
                assert_eq!(
                    values(&client.simple_query(query).await.unwrap()),
                    vec!["serializable"]
                );
            }
            client.batch_execute("ROLLBACK").await.unwrap();
            assert_eq!(
                values(
                    &client
                        .simple_query("SHOW transaction_isolation")
                        .await
                        .unwrap()
                ),
                vec!["serializable"]
            );
        }
    }
    client
        .batch_execute("SET default_transaction_isolation = 'repeatable read'")
        .await
        .unwrap();
    assert_eq!(
        values(
            &client
                .simple_query("SHOW default_transaction_isolation")
                .await
                .unwrap()
        ),
        vec!["serializable"]
    );
    drop(client);
    fixture.stop().await;
}
