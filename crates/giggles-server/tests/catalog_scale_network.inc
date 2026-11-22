async fn catalog_scale_over_pgwire(engine: giggles_core::Engine) {
    use giggles_core::roles::NewRole;
    use giggles_server::auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql};
    use std::fmt::Write as _;

    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "catalog-scale-{}-{}-{}",
        std::process::id(),
        now(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "migration_owner".into(),
            login: true,
            superuser: true,
            password: Some(Verifier::generate("fixture-password")),
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
        certificates: Default::default(),
        databases: vec!["postgres".into()],
        loopback_trust: false,
        connections: 8,
    };
    let running = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(running.address.port())
        .dbname("postgres")
        .user("migration_owner")
        .password("fixture-password")
        .ssl_mode(tokio_postgres::config::SslMode::Disable)
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    for first in (1..=200).step_by(20) {
        let mut migration = String::from("BEGIN;");
        for n in first..first + 20 {
            write!(
                migration,
                "CREATE TABLE migration_{n}(id integer PRIMARY KEY, value integer NOT NULL); \
                 CREATE INDEX migration_{n}_value ON migration_{n}(value); "
            )
            .unwrap();
        }
        migration.push_str("COMMIT;");
        client
            .batch_execute(&migration)
            .await
            .unwrap_or_else(|error| panic!("{engine:?} migration starting at {first}: {error:?}"));
    }
    assert_eq!(
        client
            .execute(
                "INSERT INTO migration_200 VALUES ($1, $2)",
                &[&7i32, &42i32]
            )
            .await
            .unwrap(),
        1
    );
    let row = client
        .query_one("SELECT id FROM migration_200 WHERE value = $1", &[&42i32])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 7);

    let namespace = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=$1)",
            &[&"public"],
        )
        .await
        .unwrap();
    assert!(namespace.get::<_, bool>(0));
    let columns = client
        .query(
            "SELECT table_name, column_name, data_type, column_default
             FROM information_schema.columns WHERE table_schema=$1 ORDER BY 1, 2",
            &[&"public"],
        )
        .await
        .unwrap();
    assert_eq!(columns.len(), 400);
    assert_eq!(columns[0].get::<_, &str>(0), "migration_1");
    assert_eq!(columns[0].get::<_, &str>(1), "id");
    assert_eq!(columns[0].get::<_, &str>(2), "integer");
    assert_eq!(columns[0].get::<_, Option<&str>>(3), None);

    for (sql, count) in [
        (
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind = 'r'",
            200,
        ),
        (
            "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public'",
            400,
        ),
        (
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'",
            200,
        ),
    ] {
        let row = client
            .query_one(sql, &[])
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert_eq!(row.get::<_, i64>(0), count, "{sql}");
    }
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn catalog_scale_legacy_migration_and_extended_queries() {
    catalog_scale_over_pgwire(giggles_core::Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn catalog_scale_v2_migration_and_extended_queries() {
    catalog_scale_over_pgwire(giggles_core::Engine::V2).await;
}
