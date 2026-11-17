#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
#[test]
fn collation_catalog_comments_privileges_and_lifecycle() {
    let path = std::env::temp_dir().join(format!(
        "user-collation-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let service = runtime.block_on(check_collation_lifecycle(&path));
    // Listener shutdown aborts socket tasks, but started spawn_blocking SQL
    // work may still own a session. Runtime shutdown waits for those workers.
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    assert!(service.upgrade().is_none(), "SQL worker retained the service");
    std::fs::remove_dir_all(path).unwrap();
}

async fn check_collation_lifecycle(path: &std::path::Path) -> std::sync::Weak<Service> {
    let admin = Admin::create_engine(path, Limits::default(), Engine::V2).unwrap();
    for name in ["owner", "other", "postgres"] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                login: true,
                superuser: name == "owner",
                ..Default::default()
            })
            .unwrap();
    }
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let stopped_service = Arc::downgrade(&service);
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
    let running = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let mut clients = Vec::new();
    let mut connections = Vec::new();
    for user in ["owner", "other"] {
        let (client, connection) = tokio_postgres::Config::new()
            .hostaddr(running.address.ip())
            .port(running.address.port())
            .user(user)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        clients.push(client);
        connections.push(tokio::spawn(connection));
    }

    let owner = &clients[0];
    let other = &clients[1];
    assert_eq!(
        other
            .batch_execute("CREATE COLLATION denied (provider=icu,locale='und')")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "42501"
    );
    owner.batch_execute("CREATE COLLATION r1 (provider=icu,locale='de@collation=phonebook'); COMMENT ON COLLATION r1 IS 'migration note'").await.unwrap();
    let row = owner
        .query_one(
            "SELECT oid,collprovider,colllocale,collversion FROM pg_collation WHERE collname='r1'",
            &[],
        )
        .await
        .unwrap();
    let oid: u32 = row.get(0);
    assert!(oid >= 16384);
    assert_eq!(row.get::<_, String>(2), "de-u-co-phonebk");
    assert_eq!(row.get::<_, String>(3), "153.128.46");
    assert_eq!(
        owner
            .query_one("SELECT pg_collation_actual_version($1::oid)", &[&oid])
            .await
            .unwrap()
            .get::<_, String>(0),
        "153.128.46"
    );
    assert_eq!(
        owner
            .query_one("SELECT obj_description($1::oid,'pg_collation')", &[&oid])
            .await
            .unwrap()
            .get::<_, String>(0),
        "migration note"
    );
    for sql in [
        "ALTER COLLATION r1 RENAME TO no",
        "DROP COLLATION r1",
        "COMMENT ON COLLATION r1 IS 'no'",
    ] {
        assert_eq!(
            other
                .batch_execute(sql)
                .await
                .unwrap_err()
                .code()
                .unwrap()
                .code(),
            "42501"
        );
    }
    owner.batch_execute("COMMENT ON COLLATION r1 IS NULL; ALTER COLLATION r1 REFRESH VERSION; ALTER COLLATION r1 OWNER TO owner; ALTER COLLATION r1 RENAME TO r2; CREATE SCHEMA rs; ALTER COLLATION r2 SET SCHEMA rs").await.unwrap();
    assert!(
        owner
            .query_one("SELECT 'a' COLLATE rs.r2 < 'b'", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        owner
            .query_one(
                "SELECT obj_description($1::oid,'pg_collation') IS NULL",
                &[&oid]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    owner
        .batch_execute("ALTER COLLATION rs.r2 OWNER TO other")
        .await
        .unwrap();
    other
        .batch_execute("COMMENT ON COLLATION rs.r2 IS 'owned'; DROP COLLATION rs.r2")
        .await
        .unwrap();
    owner.batch_execute("DROP SCHEMA rs").await.unwrap();
    for sql in [
        include_str!("../../../compat/corpus/pg_ddl/pgd_comment_collation.sql"),
        include_str!("../../../compat/corpus/pg_types/pgy_collation_lifecycle.sql"),
    ] {
        owner.batch_execute(sql).await.unwrap();
    }
    drop(clients);
    for connection in connections {
        connection.await.unwrap().unwrap();
    }
    running.shutdown().await.unwrap();
    drop(service);
    stopped_service
}
