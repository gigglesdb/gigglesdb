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
    let (client, connection) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port())
        .user("owner").dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
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
    assert_eq!(client.query_one("SELECT count(*) FROM pg_constraint WHERE contype='u'",&[]).await.unwrap().get::<_,i64>(0),0);
    client.simple_query("CREATE TABLE gui_sales.actions (x integer REFERENCES gui_sales.parent(id) ON DELETE SET NULL ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED)").await.unwrap();
    let action: String = client.query_one("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='actions_x_fkey'",&[]).await.unwrap().get(0);
    assert_eq!(action,"FOREIGN KEY (x) REFERENCES gui_sales.parent(id) ON UPDATE CASCADE ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED");
    let identity = client.query_one("SELECT a.attidentity,a.atthasdef FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='audit' AND a.attnum=1",&[]).await.unwrap();
    assert_eq!(identity.get::<_,i8>(0),b'd' as i8); assert!(!identity.get::<_,bool>(1));
    client.simple_query("CREATE TABLE gui_sales.check_names (id integer PRIMARY KEY, lower text, text text, \"true\" boolean, CHECK (lower('x') = 'x' AND 'a' = 'a' AND true))").await.unwrap();
    assert!(client.query_one("SELECT conkey IS NULL FROM pg_constraint WHERE conname='check_names_check'",&[]).await.unwrap().get::<_,bool>(0));
    drop(client); connection.await.unwrap().unwrap(); running.shutdown().await.unwrap(); drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
