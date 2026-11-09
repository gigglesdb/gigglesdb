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
            "psql-table-owner-{}-{}-{}",
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
            .grant_schema("public", "reader", &[SchemaPrivilege::Create, SchemaPrivilege::Usage])
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
fn rows(messages: &[SimpleQueryMessage]) -> Vec<Vec<String>> {
    messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).unwrap_or("NULL").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// Exact `\dt <pattern>` query psql 18.6 sends (captured with `psql -E`).
fn psql_dt(pattern: &str) -> String {
    format!(
        "SELECT n.nspname as \"Schema\",\n  c.relname as \"Name\",\n  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\nFROM pg_catalog.pg_class c\n     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam\nWHERE c.relkind IN ('r','p','t','s','')\n  AND c.relname OPERATOR(pg_catalog.~) '^({pattern})$' COLLATE pg_catalog.default\n  AND pg_catalog.pg_table_is_visible(c.oid)\nORDER BY 1,2;"
    )
}

fn row(name: &str, owner: &str) -> Vec<String> {
    ["public", name, "table", owner].map(str::to_owned).to_vec()
}

#[tokio::test]
async fn psql_dt_names_the_creating_role_as_owner() {
    let fixture = Fixture::new().await;
    let owner = fixture.client("owner").await;
    let reader = fixture.client("reader").await;
    owner
        .batch_execute("CREATE TABLE r0_owned (id integer PRIMARY KEY, name text)")
        .await
        .unwrap();
    reader
        .batch_execute("CREATE TABLE r0_read (id integer PRIMARY KEY)")
        .await
        .unwrap();
    // PostgreSQL 18.6 prints the creating role (`postgres` in the R0 oracle).
    assert_eq!(
        rows(&owner.simple_query(&psql_dt("r0_owned")).await.unwrap()),
        [row("r0_owned", "owner")]
    );
    assert_eq!(
        rows(&owner.simple_query(&psql_dt("r0_.*")).await.unwrap()),
        [row("r0_owned", "owner"), row("r0_read", "reader")]
    );
    // Output stays authorization-filtered: the reader sees only its own table.
    assert_eq!(
        rows(&reader.simple_query(&psql_dt("r0_.*")).await.unwrap()),
        [row("r0_read", "reader")]
    );
    let roles = rows(
        &reader
            .simple_query(
                "SELECT rolname, rolsuper::text FROM pg_catalog.pg_roles \
                 WHERE rolname IN ('giggles', 'owner', 'reader') ORDER BY 1",
            )
            .await
            .unwrap(),
    );
    assert_eq!(
        roles,
        [
            vec!["giggles".to_owned(), "true".to_owned()],
            vec!["owner".to_owned(), "true".to_owned()],
            vec!["reader".to_owned(), "false".to_owned()],
        ]
    );
    drop((owner, reader));
    fixture.stop().await;
}
