use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{Service, auth::{PostgresqlConfig, TlsMode, serve_postgresql}, clock::Clock};
use std::{collections::BTreeMap, sync::Arc};
#[tokio::test]
async fn stored_constraint_metadata_reaches_simple_and_extended_wire() {
    let path = std::env::temp_dir().join(format!("cs2-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin.roles().create(NewRole { name: "owner".into(), login: true, superuser: true, ..Default::default() }).unwrap();
    let service = Service::new(admin, vec![], Clock::new(0,3600).unwrap()).unwrap();
    let config = PostgresqlConfig { listen: "127.0.0.1:0".parse().unwrap(), tls: TlsMode::Disabled,
        certificate: None, private_key: None, client_ca: None, certificates: BTreeMap::new(),
        databases: vec!["postgres".into()], loopback_trust: true, connections: 8 };
    let running = serve_postgresql(config.listen, service.clone(), Arc::new(config.build().unwrap())).await.unwrap();
    let connect = |port| {
        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1").port(port).user("owner").dbname("postgres");
        config
    };
    let (client, connection) = connect(running.address.port()).connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    for sql in include_str!("../../../compat/gui/sample.sql").split(';').map(str::trim).filter(|s| s.starts_with("CREATE SCHEMA") || s.starts_with("CREATE TABLE") || s.starts_with("CREATE INDEX")) {
        client.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    for sql in ["SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE contype='c'",
        "SELECT pg_get_expr(adbin,adrelid) FROM pg_attrdef ORDER BY adnum DESC"] {
        let simple: Vec<_> = client.simple_query(sql).await.unwrap().into_iter().filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_owned()), _=>None }).collect();
        let extended: Vec<String> = client.query(sql,&[]).await.unwrap().iter().map(|r| r.get(0)).collect();
        assert_eq!(simple,extended);
        if sql.contains("constraint") { assert_eq!(simple,["CHECK ((amount >= 0))"]); }
        else { assert_eq!(simple,["7", "'sample'::text"]); }
    }
    let fk: String = client.query_one("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE contype='f'",&[]).await.unwrap().get(0);
    assert_eq!(fk,"FOREIGN KEY (parent_id) REFERENCES gui_sales.parent(id)");
    assert_eq!(client.query_one("SELECT count(*) FROM pg_constraint WHERE contype='u'",&[]).await.unwrap().get::<_,i64>(0),2);
    let uq: String = client.query_one("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='child_label_key'",&[]).await.unwrap().get(0);
    assert_eq!(uq, "UNIQUE (label)");
    client.simple_query("CREATE TABLE gui_sales.uq_drop (label text CONSTRAINT owned UNIQUE)").await.unwrap();
    let error = client.simple_query("DROP INDEX gui_sales.owned").await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "2BP01");
    client.simple_query("ALTER TABLE gui_sales.uq_drop DROP CONSTRAINT owned").await.unwrap();
    client.simple_query("CREATE TABLE gui_sales.actions (x integer REFERENCES gui_sales.parent(id) ON DELETE SET NULL ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED)").await.unwrap();
    let action: String = client.query_one("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='actions_x_fkey'",&[]).await.unwrap().get(0);
    assert_eq!(action,"FOREIGN KEY (x) REFERENCES gui_sales.parent(id) ON UPDATE CASCADE ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED");
    let identity = client.query_one("SELECT a.attidentity,a.atthasdef FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='audit' AND a.attnum=1",&[]).await.unwrap();
    assert_eq!(identity.get::<_,i8>(0),b'd' as i8); assert!(!identity.get::<_,bool>(1));
    client.simple_query("CREATE TABLE gui_sales.check_names (id integer PRIMARY KEY, lower text, text text, \"true\" boolean, CHECK (lower('x') = 'x' AND 'a' = 'a' AND true))").await.unwrap();
    assert!(client.query_one("SELECT conkey IS NULL FROM pg_constraint WHERE conname='check_names_check'",&[]).await.unwrap().get::<_,bool>(0));
    for (table, extended) in [("migration_simple", false), ("migration_extended", true)] {
        for sql in [
            "BEGIN".into(),
            format!("CREATE TABLE {table} (id int PRIMARY KEY, n int CHECK (n > 0), m int, CHECK (n < m))"),
            format!("ALTER TABLE {table} RENAME COLUMN n TO n2"),
            "COMMIT".into(),
        ] {
            migration_statement(&client, &sql, extended).await.unwrap();
        }
    }
    drop(client); connection.await.unwrap().unwrap(); running.shutdown().await.unwrap(); drop(service);

    let admin = Admin::open(&path, Limits::default()).unwrap();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let running = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap())).await.unwrap();
    let (client, connection) = connect(running.address.port()).connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    for (table, extended) in [("migration_simple", false), ("migration_extended", true)] {
        migration_statement(&client, &format!("INSERT INTO {table} VALUES (1, 2, 3)"), extended).await.unwrap();
        let error = migration_statement(&client, &format!("INSERT INTO {table} VALUES (2, 0, 3)"), extended)
            .await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "23514");
        assert_eq!(error.constraint(), Some(format!("{table}_n_check").as_str()));
        assert_eq!(error.detail(), Some("Failing row contains (2, 0, 3)."));
        migration_statement(&client, &format!("ALTER TABLE {table} DROP COLUMN m"), extended).await.unwrap();
        migration_statement(&client, &format!("INSERT INTO {table} VALUES (2, 4)"), extended).await.unwrap();
        let sql = format!("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='{table}'::regclass AND contype='c'");
        let row = client.query_one(&sql, &[]).await.unwrap();
        assert_eq!(row.get::<_, &str>(0), "CHECK ((n2 > 0))");
    }
    drop(client); connection.await.unwrap().unwrap(); running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

async fn migration_statement(client: &tokio_postgres::Client, sql: &str, extended: bool) -> Result<(), tokio_postgres::Error> {
    if extended {
        client.execute(sql, &[]).await?;
    } else {
        client.simple_query(sql).await?;
    }
    Ok(())
}
