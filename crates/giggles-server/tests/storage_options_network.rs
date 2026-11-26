use super::session_settings_network::{wire_cycle, wire_query};
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
    pgwire::z,
};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

async fn start(admin: Admin) -> Running {
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
    serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap()
}

async fn connect(server: &Running) -> Client {
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(server.address.ip())
        .port(server.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
}

async fn ddl(server: &Running) {
    let mut stream = TcpStream::connect(server.address).await.unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    for (key, value) in [("user", "owner"), ("database", "postgres")] {
        z(key, &mut startup);
        z(value, &mut startup);
    }
    startup.push(0);
    stream.write_u32((startup.len() + 4) as u32).await.unwrap();
    stream.write_all(&startup).await.unwrap();
    wire_cycle(&mut stream).await;

    let frames = wire_query(&mut stream, "ALTER TABLE IF EXISTS missing ADD COLUMN v int").await;
    assert_eq!(frames.iter().map(|frame| frame.0).collect::<Vec<_>>(), b"NCZ");
    assert_eq!(
        frames[0].1,
        b"SNOTICE\0VNOTICE\0C00000\0Mrelation \"missing\" does not exist, skipping\0\0"
    );
    assert_eq!(frames[1].1, b"ALTER TABLE\0");

    let frames = wire_query(&mut stream, "SELECT 1").await;
    assert_eq!(frames.iter().map(|frame| frame.0).collect::<Vec<_>>(), b"TDCZ");
    assert_eq!(frames[1].1, b"\0\x01\0\0\0\x011");
    assert_eq!(frames[2].1, b"SELECT 1\0");

    for (sql, tag) in [
        (
            "CREATE TABLE t (id int PRIMARY KEY, v text) WITH (fillfactor=70, toast.autovacuum_enabled=false)",
            "CREATE TABLE",
        ),
        (
            "CREATE UNLOGGED TABLE u (id int PRIMARY KEY, v text)",
            "CREATE TABLE",
        ),
        ("INSERT INTO u VALUES (1, 'durable')", "INSERT 0 1"),
        (
            "CREATE INDEX t_v ON t(v) WITH (fillfactor=80)",
            "CREATE INDEX",
        ),
        (
            "ALTER TABLE t SET (autovacuum_enabled=false), CLUSTER ON t_v",
            "ALTER TABLE",
        ),
        ("ALTER INDEX t_v RENAME TO t_value", "ALTER INDEX"),
        (
            "ALTER TABLE t ALTER COLUMN v SET STATISTICS 200, ALTER COLUMN v SET STORAGE EXTERNAL, ALTER COLUMN v SET COMPRESSION pglz, ALTER COLUMN v SET (n_distinct=0.5)",
            "ALTER TABLE",
        ),
    ] {
        let frames = wire_query(&mut stream, sql).await;
        assert!(
            !frames.iter().any(|frame| frame.0 == b'E'),
            "{sql}: {frames:?}"
        );
        assert_eq!(
            frames.iter().find(|frame| frame.0 == b'C').unwrap().1,
            format!("{tag}\0").as_bytes(),
            "{sql}"
        );
    }
}

async fn rows(client: &Client, query: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(query)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"))
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|col| row.get(col).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn check(path: &Path, admin: Admin, set_up: bool) {
    let server = start(admin).await;
    if set_up {
        ddl(&server).await;
    }
    let client = connect(&server).await;
    let s = |value: &str| Some(value.to_owned());
    let relations = "SELECT c.relname, c.relpersistence, c.reloptions FROM pg_catalog.pg_class c WHERE c.relname IN ('t','t_pkey','t_value','u','u_pkey') ORDER BY c.relname";
    let indexes = "SELECT i.indisclustered, pg_catalog.pg_get_indexdef(i.indexrelid) FROM pg_catalog.pg_index i WHERE i.indexrelid='t_value'::regclass";
    let columns = "SELECT a.attstattarget,a.attstorage,a.attcompression,a.attoptions FROM pg_catalog.pg_attribute a WHERE a.attrelid='t'::regclass AND a.attname='v'";
    let expected_relations = [
        vec![
            s("t"),
            s("p"),
            s("{fillfactor=70,autovacuum_enabled=false}"),
        ],
        vec![s("t_pkey"), s("p"), None],
        vec![s("t_value"), s("p"), s("{fillfactor=80}")],
        vec![s("u"), s("u"), None],
        vec![s("u_pkey"), s("u"), None],
    ];
    let expected_indexes = [vec![
        s("t"),
        s("CREATE INDEX t_value ON public.t USING btree (v) WITH (fillfactor='80')"),
    ]];
    let expected_columns = [vec![s("200"), s("e"), s("p"), s("{n_distinct=0.5}")]];
    assert_eq!(rows(&client, relations).await, expected_relations);
    assert_eq!(rows(&client, indexes).await, expected_indexes);
    assert_eq!(rows(&client, columns).await, expected_columns);
    if !set_up {
        client.batch_execute("BEGIN").await.unwrap();
        client
            .batch_execute("ALTER TABLE t SET UNLOGGED, SET WITHOUT CLUSTER")
            .await
            .unwrap();
        assert_eq!(
            rows(
                &client,
                "SELECT relpersistence, reloptions FROM pg_catalog.pg_class WHERE relname='t_value'"
            )
            .await,
            [vec![s("u"), s("{fillfactor=80}")]],
        );
        client
            .batch_execute("ALTER INDEX t_value RESET (fillfactor)")
            .await
            .unwrap();
        client
            .batch_execute("ALTER TABLE t ALTER COLUMN v RESET (n_distinct)")
            .await
            .unwrap();
        assert_eq!(
            rows(&client, relations).await,
            [
                vec![
                    s("t"),
                    s("u"),
                    s("{fillfactor=70,autovacuum_enabled=false}")
                ],
                vec![s("t_pkey"), s("u"), None],
                vec![s("t_value"), s("u"), None],
                vec![s("u"), s("u"), None],
                vec![s("u_pkey"), s("u"), None],
            ],
        );
        assert_eq!(
            rows(&client, indexes).await,
            [vec![
                s("f"),
                s("CREATE INDEX t_value ON public.t USING btree (v)"),
            ]],
        );
        assert_eq!(
            rows(&client, columns).await,
            [vec![s("200"), s("e"), s("p"), None]],
        );
        client.batch_execute("DROP INDEX t_value").await.unwrap();
        client
            .batch_execute("ALTER TABLE t DROP COLUMN v")
            .await
            .unwrap();
        assert!(rows(&client, columns).await.is_empty());
        client.batch_execute("ROLLBACK").await.unwrap();
        assert_eq!(rows(&client, relations).await, expected_relations);
        assert_eq!(rows(&client, indexes).await, expected_indexes);
        assert_eq!(rows(&client, columns).await, expected_columns);
    }
    assert_eq!(
        rows(&client, "SELECT id,v FROM u").await,
        [vec![s("1"), s("durable")]]
    );
    drop(client);
    server.shutdown().await.unwrap();
    assert!(path.exists());
}

#[tokio::test]
async fn pg_dump_catalogs_keep_storage_metadata_across_restarts() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "storage-options-pgwire-{}-{nonce}",
        std::process::id()
    ));
    let limits = Limits::default();
    let admin = Admin::create_engine(&path, limits, Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            superuser: true,
            login: true,
            ..NewRole::default()
        })
        .unwrap();
    check(&path, admin, true).await;
    check(&path, Admin::open(&path, limits).unwrap(), false).await;
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn unique_storage_index_rename_updates_catalogs_and_enforces_after_reopen() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "unique-storage-index-{}-{nonce}",
        std::process::id()
    ));
    let limits = Limits::default();
    let admin = Admin::create_engine(&path, limits, Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            superuser: true,
            login: true,
            ..NewRole::default()
        })
        .unwrap();
    let mut sql = admin.sql_session();
    for statement in [
        "CREATE TABLE accounts (id int PRIMARY KEY, email text UNIQUE)",
        "ALTER INDEX accounts_email_key SET (fillfactor=75)",
        "INSERT INTO accounts VALUES (1, 'existing')",
        "ALTER INDEX accounts_email_key RENAME TO accounts_email_unique",
    ] {
        sql.execute(statement, &[], None)
            .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
    }
    drop((sql, admin));

    let server = start(Admin::open(&path, limits).unwrap()).await;
    let client = connect(&server).await;
    assert_eq!(
        rows(&client, "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid='accounts'::regclass AND contype='u'").await,
        [vec![Some("accounts_email_unique".into())]],
    );
    assert_eq!(
        rows(&client, "SELECT indexname, indexdef FROM pg_catalog.pg_indexes WHERE tablename='accounts' AND indexname <> 'accounts_pkey'").await,
        [vec![
            Some("accounts_email_unique".into()),
            Some("CREATE UNIQUE INDEX accounts_email_unique ON public.accounts USING btree (email) WITH (fillfactor='75')".into()),
        ]],
    );
    let error = client
        .simple_query("INSERT INTO accounts VALUES (2, 'existing')")
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23505");
    assert_eq!(error.constraint(), Some("accounts_email_unique"));
    assert_eq!(
        rows(&client, "SELECT id FROM accounts WHERE email='existing'").await,
        [vec![Some("1".into())]],
    );
    drop(client);
    server.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
