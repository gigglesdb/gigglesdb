#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{Service, auth::{PostgresqlConfig, TlsMode, serve_postgresql}, clock::Clock};
use std::{collections::BTreeMap, sync::Arc};
#[tokio::test]
async fn views_use_private_authorized_typed_catalog_snapshots() {
    let path = std::env::temp_dir().join(format!("view-presentation-{}-{}", std::process::id(),
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
    owner.batch_execute("CREATE SCHEMA s; CREATE TABLE s.t(id int PRIMARY KEY,amount numeric(10,2)); BEGIN; CREATE VIEW s.v AS SELECT id,amount FROM s.t; COMMENT ON VIEW s.v IS 'private view'").await.unwrap();
    let oid: u32 = owner.query_one("SELECT oid FROM pg_class WHERE relname='v'",&[]).await.unwrap().get(0);
    let row = owner.query_one("SELECT relkind,relam,relnatts FROM pg_class WHERE oid=$1",&[&oid]).await.unwrap();
    assert_eq!((row.get::<_,i8>(0),row.get::<_,u32>(1),row.get::<_,i16>(2)),(b'v' as i8,0,2));
    assert_eq!(owner.query_one("SELECT count(*) FROM pg_attribute WHERE attrelid=$1",&[&oid]).await.unwrap().get::<_,i64>(0),2);
    let statement = owner.prepare("SELECT pg_get_viewdef($1::oid),obj_description($1::oid)").await.unwrap();
    let row = owner.query_one(&statement,&[&oid]).await.unwrap();
    assert!(row.get::<_,String>(0).contains("SELECT")); assert_eq!(row.get::<_,String>(1),"private view");
    assert_eq!(owner.query_one("SELECT table_schema,table_type FROM information_schema.tables WHERE table_name='v'",&[]).await.unwrap().get::<_,String>(0),"s");
    assert_eq!(owner.query_one("SELECT numeric_precision FROM information_schema.columns WHERE table_name='v' AND column_name='amount'",&[]).await.unwrap().get::<_,i32>(0),10);
    assert_eq!(clients[1].query_one("SELECT count(*) FROM pg_views WHERE viewname='v'",&[]).await.unwrap().get::<_,i64>(0),0);
    assert_eq!(owner.query_one("SELECT viewowner FROM pg_views WHERE viewname='v'",&[]).await.unwrap().get::<_,String>(0),"owner");
    owner.batch_execute("COMMENT ON COLUMN s.v.amount IS 'output'; COMMIT").await.unwrap();
    assert_eq!(owner.query_one("SELECT viewowner FROM pg_views WHERE viewname='v'",&[]).await.unwrap().get::<_,String>(0),"owner");
    assert_eq!(clients[1].query_one("SELECT pg_get_viewdef($1::oid)",&[&oid]).await.unwrap().get::<_,Option<String>>(0),None);
    owner.batch_execute("BEGIN; DROP VIEW s.v").await.unwrap();
    assert_eq!(owner.query_one(&statement,&[&oid]).await.unwrap().get::<_,Option<String>>(0),None);
    owner.batch_execute("ROLLBACK").await.unwrap();
    assert!(owner.query_one(&statement,&[&oid]).await.unwrap().get::<_,String>(0).contains("SELECT"));
    drop(clients); for connection in connections { connection.await.unwrap().unwrap(); }
    running.shutdown().await.unwrap(); drop(service); std::fs::remove_dir_all(path).unwrap();
}
