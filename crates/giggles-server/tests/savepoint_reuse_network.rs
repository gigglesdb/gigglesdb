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
use tokio_postgres::{Client, Config, NoTls, config::SslMode};

static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    service: Arc<Service>,
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new(engine: Engine) -> Self {
        let path = std::env::temp_dir().join(format!(
            "transaction-modes-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap();
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
async fn reuse(engine: Engine) {
    let f = Fixture::new(engine).await;
    let c = f.client().await;
    c.batch_execute("CREATE TABLE items(id integer PRIMARY KEY)")
        .await
        .unwrap();
    let one = c.prepare("SELECT 1 n").await.unwrap();
    c.query_one(&one, &[]).await.unwrap();
    c.batch_execute("INSERT INTO items VALUES (1)")
        .await
        .unwrap();
    assert_eq!(c.query_one(&one, &[]).await.unwrap().get::<_, i32>(0), 1);
    c.batch_execute("INSERT INTO items VALUES (2)").await.unwrap();
    c.batch_execute("BEGIN").await.unwrap();
    assert_eq!(c.query_one(&one, &[]).await.unwrap().get::<_, i32>(0), 1);
    c.batch_execute("SAVEPOINT expected").await.unwrap();
    let error = c
        .batch_execute("INSERT INTO items VALUES (1)")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "23505");
    c.batch_execute("ROLLBACK TO expected").await.unwrap();
    assert_eq!(c.query_one(&one, &[]).await.unwrap().get::<_, i32>(0), 1);
    c.batch_execute("COMMIT").await.unwrap();
    drop(c);
    f.stop().await;
}
#[tokio::test]
async fn legacy() {
    reuse(Engine::Legacy).await;
}
#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn v2() {
    reuse(Engine::V2).await;
}
