#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{Service, auth::{PostgresqlConfig, TlsMode, serve_postgresql}, clock::Clock};
use std::{collections::BTreeMap, sync::Arc};
#[tokio::test]
async fn comments_are_private_authorized_and_use_current_attnum() {
    let path = std::env::temp_dir().join(format!("comment-presentation-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    for name in ["owner", "other"] {
        admin.roles().create(NewRole { name: name.into(), login: true, superuser: name == "owner", ..Default::default() }).unwrap();
    }
    let service = Service::new(admin, vec![], Clock::new(0,3600).unwrap()).unwrap();
    let config = PostgresqlConfig { listen: "127.0.0.1:0".parse().unwrap(), tls: TlsMode::Disabled,
        certificate: None, private_key: None, client_ca: None, certificates: BTreeMap::new(),
        databases: vec!["postgres".into()], loopback_trust: true, connections: 8 };
    let running = serve_postgresql(config.listen, service.clone(), Arc::new(config.build().unwrap())).await.unwrap();
    let mut clients = Vec::new();
    let mut connections = Vec::new();
    for user in ["owner", "other"] {
        let (client, connection) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port())
            .user(user).dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
        clients.push(client); connections.push(tokio::spawn(connection));
    }
    let owner = &clients[0];
    owner.batch_execute("CREATE TABLE t(a int,b int); BEGIN; COMMENT ON TABLE t IS 'table'; COMMENT ON COLUMN t.b IS 'column'").await.unwrap();
    let oid: u32 = owner.query_one("SELECT oid FROM pg_class WHERE relname='t'",&[]).await.unwrap().get(0);
    let sql = format!("SELECT obj_description({oid}::oid),obj_description({oid}::oid,'pg_class'),col_description({oid}::oid,2),col_description({oid}::oid,1)");
    for (client, expected) in [(owner, vec![Some("table"),Some("table"),Some("column"),None]), (&clients[1],vec![None;4])] {
        let row = client.query_one(&sql,&[]).await.unwrap();
        assert_eq!((0..4).map(|i| row.get::<_,Option<&str>>(i)).collect::<Vec<_>>(),expected);
    }
    assert_eq!(owner.query_one("SELECT count(*) FROM pg_description WHERE classoid=1259",&[]).await.unwrap().get::<_,i64>(0),2);
    assert_eq!(clients[1].query_one("SELECT count(*) FROM pg_description WHERE classoid=1259",&[]).await.unwrap().get::<_,i64>(0),0);
    owner.batch_execute("COMMIT; BEGIN; ALTER TABLE t DROP COLUMN a").await.unwrap();
    let sql = format!("SELECT col_description({oid}::oid,1),col_description({oid}::oid,2)");
    let rows = owner.simple_query(&sql).await.unwrap();
    let row = rows.iter().find_map(|r| match r { tokio_postgres::SimpleQueryMessage::Row(r)=>Some(r),_=>None }).unwrap();
    assert_eq!((row.get(0),row.get(1)),(Some("column"),None));
    owner.batch_execute("ROLLBACK; BEGIN; CREATE TABLE private_t(a int); COMMENT ON TABLE private_t IS 'private'").await.unwrap();
    assert_eq!(owner.query_one("SELECT obj_description(oid) FROM pg_class WHERE relname='private_t'",&[]).await.unwrap().get::<_,String>(0),"private");
    owner.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(owner.query_one("SELECT col_description($1::oid,2)",&[&oid]).await.unwrap().get::<_,String>(0),"column");
    drop(clients); for connection in connections { connection.await.unwrap().unwrap(); }
    running.shutdown().await.unwrap();
    drop(service);
    // Windows can briefly report a sharing violation during engine teardown.
    // Bound the wait and keep every other removal failure visible.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match std::fs::remove_dir_all(&path) {
            Ok(()) => break,
            Err(error) if cfg!(windows) && error.raw_os_error() == Some(32)
                && std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(error) => panic!("remove {} after engine shutdown: {error}", path.display()),
        }
    }
}
