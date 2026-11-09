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
use tokio_postgres::{Client, Config, NoTls, config::SslMode, types::Type};
static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    service: Arc<Service>,
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "npgsql-get-schema-{}-{}-{}",
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

// Verbatim Npgsql 9.0.4 NpgsqlSchema.GetTables / GetColumns text after Npgsql
// rewrites its `:name` restriction placeholders to `$n`. Npgsql binds every
// restriction as text (OID 25) while the catalog infers `name` for the
// information_schema identifier columns; PostgreSQL 18 resolves both as text
// comparisons.
const TABLES_SQL: &str = "SELECT table_catalog, table_schema, table_name, table_type\nFROM information_schema.tables\nWHERE\n    table_type IN ('BASE TABLE', 'FOREIGN', 'FOREIGN TABLE') AND\n    table_schema NOT IN ('pg_catalog', 'information_schema') AND table_schema = $1 AND table_name = $2 AND table_type = $3";

const COLUMNS_SQL: &str = "SELECT\n    table_catalog,\n    table_schema,\n    table_name,\n    column_name,\n    ordinal_position,\n    column_default,\n    is_nullable,\n    CASE WHEN udt_schema is NULL THEN udt_name ELSE format_type(typ.oid, NULL) END,\n    character_maximum_length,\n    character_octet_length,\n    numeric_precision,\n    numeric_precision_radix,\n    numeric_scale,\n    datetime_precision,\n    character_set_catalog,\n    character_set_schema,\n    character_set_name,\n    collation_catalog\nFROM information_schema.columns\nJOIN pg_namespace AS ns ON ns.nspname = udt_schema\nJOIN pg_type AS typ ON typnamespace = ns.oid AND typname = udt_name\n WHERE table_schema = $1 AND table_name = $2";

#[tokio::test]
async fn npgsql_get_schema_tables_and_columns_bind_text_restrictions() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE r0_items (id integer PRIMARY KEY, name text, score bigint)")
        .await
        .unwrap();

    for declared in [Type::TEXT, Type::VARCHAR] {
        let tables = c
            .prepare_typed(
                TABLES_SQL,
                &[declared.clone(), declared.clone(), declared.clone()],
            )
            .await
            .unwrap();
        assert_eq!(
            tables.params(),
            &[declared.clone(), declared.clone(), declared.clone()]
        );
        let rows = c
            .query(&tables, &[&"public", &"r0_items", &"BASE TABLE"])
            .await
            .unwrap();
        let observed: Vec<Vec<String>> = rows
            .iter()
            .map(|r| (0..4).map(|i| r.get::<_, String>(i)).collect())
            .collect();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0][1..], ["public", "r0_items", "BASE TABLE"]);

        let columns = c
            .prepare_typed(COLUMNS_SQL, &[declared.clone(), declared.clone()])
            .await
            .unwrap();
        let rows = c.query(&columns, &[&"public", &"r0_items"]).await.unwrap();
        // Exactly the declared columns: no hidden storage column is exposed.
        let observed: Vec<(String, i32, String, String)> = rows
            .iter()
            .map(|r| (r.get(3), r.get(4), r.get(6), r.get(7)))
            .collect();
        assert_eq!(
            observed,
            [
                ("id".into(), 1, "NO".into(), "integer".into()),
                ("name".into(), 2, "YES".into(), "text".into()),
                ("score".into(), 3, "YES".into(), "bigint".into()),
            ]
        );
        // A restriction that matches nothing yields no rows, not an error.
        let rows = c.query(&columns, &[&"public", &"absent"]).await.unwrap();
        assert!(rows.is_empty());
    }

    // Cross-family declarations stay rejected.
    let error = c
        .prepare_typed(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = $1",
            &[Type::INT4],
        )
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "0A000");
    drop(c);
    f.stop().await;
}
