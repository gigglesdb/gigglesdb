use giggles_core::{
    Admin, Limits,
    roles::{NewRole, SchemaPrivilege},
};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, config::SslMode};

struct Fixture {
    service: Arc<Service>,
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "mixed-catalog-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
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
        admin
            .roles()
            .create(NewRole {
                name: "reader".into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        admin
            .roles()
            .grant_schema(
                "public",
                "reader",
                &[SchemaPrivilege::Create, SchemaPrivilege::Usage],
            )
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
    async fn client(&self, user: &str) -> Client {
        let (client, connection) = Config::new()
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
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

const QUERY: &str = "SELECT c.relname, count(t.*) FROM pg_catalog.pg_class c LEFT JOIN app t ON true WHERE c.relname = 'app' GROUP BY c.relname ORDER BY c.relname";
#[tokio::test]
async fn mixed_catalog_checks_table_privileges_and_counts_outer_rows() {
    let fixture = Fixture::new().await;
    let owner = fixture.client("owner").await;
    let reader = fixture.client("reader").await;
    owner
        .batch_execute("CREATE TABLE app (id integer)")
        .await
        .unwrap();
    for sql in ["SELECT * FROM app", QUERY] {
        assert_eq!(
            sqlstate(reader.simple_query(sql).await.unwrap_err()),
            "42501"
        );
        assert_eq!(sqlstate(reader.query(sql, &[]).await.unwrap_err()), "42501");
    }
    for expected in [0i64, 1] {
        let rows = owner.query(QUERY, &[]).await.unwrap();
        assert_eq!(rows[0].get::<_, i64>(1), expected);
        let rows = owner.simple_query(QUERY).await.unwrap();
        let row = rows
            .iter()
            .find_map(|m| {
                if let SimpleQueryMessage::Row(row) = m {
                    Some(row)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(row.get(1), Some(expected.to_string().as_str()));
        if expected == 0 {
            owner
                .batch_execute("INSERT INTO app VALUES (NULL)")
                .await
                .unwrap();
        }
    }
    owner
        .batch_execute("GRANT SELECT ON app TO reader")
        .await
        .unwrap();
    let prepared = reader.prepare(QUERY).await.unwrap();
    assert_eq!(
        reader.query(&prepared, &[]).await.unwrap()[0].get::<_, i64>(1),
        1
    );
    owner
        .batch_execute("REVOKE SELECT ON app FROM reader")
        .await
        .unwrap();
    assert_eq!(
        sqlstate(reader.query(&prepared, &[]).await.unwrap_err()),
        "42501"
    );
    drop((reader, owner));
    fixture.stop().await;
}

fn sqlstate(error: tokio_postgres::Error) -> String {
    error.code().unwrap().code().to_owned()
}
