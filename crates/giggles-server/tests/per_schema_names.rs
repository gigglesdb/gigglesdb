use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::{Config, NoTls};

#[tokio::test]
async fn pg_class_distinguishes_same_named_relations_and_grants() {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path =
        std::env::temp_dir().join(format!("per-schema-wire-{:x}", u128::from_le_bytes(nonce)));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
    for (name, superuser) in [("owner", true), ("reader", false)] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                superuser,
                login: true,
                ..NewRole::default()
            })
            .unwrap();
    }
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
    let pg = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let connect = |user: &str| {
        let mut config = Config::new();
        config
            .host("localhost")
            .hostaddr(pg.address.ip())
            .port(pg.address.port())
            .user(user)
            .dbname("postgres");
        config
    };
    let (owner, connection) = connect("owner").connect(NoTls).await.unwrap();
    let owner_connection = tokio::spawn(connection);
    for sql in [
        "CREATE SCHEMA a",
        "CREATE SCHEMA b",
        "CREATE TABLE a.t(id int PRIMARY KEY)",
        "CREATE TABLE b.t(id int PRIMARY KEY)",
        "INSERT INTO a.t VALUES(1)",
        "INSERT INTO b.t VALUES(2)",
        "CREATE INDEX ti ON a.t(id)",
        "CREATE INDEX ti ON b.t(id)",
    ] {
        owner
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let rows = owner.query("SELECT c.relname, n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='t' ORDER BY n.nspname", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        (rows[0].get::<_, String>(0), rows[0].get::<_, String>(1)),
        ("t".into(), "a".into())
    );
    assert_eq!(
        (rows[1].get::<_, String>(0), rows[1].get::<_, String>(1)),
        ("t".into(), "b".into())
    );
    // The relation/namespace join used by psql's describe queries sees both indexes too.
    let rows = owner.query("SELECT c.relname, n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='ti' ORDER BY n.nspname", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    owner
        .batch_execute("GRANT SELECT ON a.t TO reader")
        .await
        .unwrap();
    let (reader, connection) = connect("reader").connect(NoTls).await.unwrap();
    let reader_connection = tokio::spawn(connection);
    assert_eq!(
        reader
            .query_one("SELECT id FROM a.t", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    assert_eq!(
        reader
            .query("SELECT id FROM b.t", &[])
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "42501"
    );
    reader
        .batch_execute("SET search_path=a,b,public")
        .await
        .unwrap();
    let prepared = reader.prepare("SELECT id FROM t").await.unwrap();
    reader
        .batch_execute("SET search_path=b,a,public")
        .await
        .unwrap();
    assert_eq!(
        reader
            .query(&prepared, &[])
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "42501"
    );
    owner.batch_execute("DROP TABLE b.t").await.unwrap();
    assert_eq!(
        owner
            .query_one("SELECT id FROM a.t", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    drop(reader);
    drop(owner);
    reader_connection.await.unwrap().unwrap();
    owner_connection.await.unwrap().unwrap();
    pg.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
