#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn view_select_simple_extended_authority_and_drop() {
    use giggles_core::{Admin, Engine, Limits, roles::{NewRole, TablePrivilege}};
    use giggles_server::{Service, auth::{PostgresqlConfig, TlsMode, serve_postgresql}, clock::Clock};
    use std::{collections::BTreeMap, sync::Arc};
    let path = std::env::temp_dir().join(format!("view-select-wire-{}",std::process::id()));
    let admin = Admin::create_engine(&path,Limits::default(),Engine::V2).unwrap();
    admin.roles().create(NewRole { name:"reader".into(),login:true,..Default::default() }).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE t(a int,b int)","INSERT INTO t VALUES(1,2)","CREATE VIEW v(x,y) AS SELECT b,a FROM t"] { s.execute(sql,&[],None).unwrap(); }
    admin.roles().grant_table("v","reader",&[TablePrivilege::Select]).unwrap();
    let service = Service::new(admin.clone(),vec![],Clock::new(0,3600).unwrap()).unwrap();
    let config = PostgresqlConfig { listen:"127.0.0.1:0".parse().unwrap(),tls:TlsMode::Disabled,
        certificate:None,private_key:None,client_ca:None,certificates:BTreeMap::new(),databases:vec!["postgres".into()],loopback_trust:true,connections:8 };
    let running = serve_postgresql(config.listen,service.clone(),Arc::new(config.build().unwrap())).await.unwrap();
    let (client,connection) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port()).user("reader").dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    for extended in [false,true] {
        let error = if extended { client.query("SELECT missing FROM v",&[]).await.err() } else { client.simple_query("SELECT missing FROM v").await.err() }.unwrap();
        assert_eq!(error.code().unwrap().code(),"42501");
    }
    admin.roles().grant_table("t","reader",&[TablePrivilege::Select]).unwrap();
    for extended in [false,true] {
        if extended {
            let statement = client.prepare("SELECT * FROM v WHERE y=$1 ORDER BY x LIMIT 1").await.unwrap();
            assert_eq!(statement.columns().iter().map(|c| c.name()).collect::<Vec<_>>(),["x","y"]);
            assert_eq!(client.query(&statement,&[&1i32]).await.unwrap()[0].get::<_,i32>(0),2);
        } else {
            let rows = client.simple_query("SELECT * FROM v ORDER BY x").await.unwrap();
            assert!(rows.iter().any(|r| matches!(r,tokio_postgres::SimpleQueryMessage::Row(r) if r.get(0)==Some("2") && r.get(1)==Some("1"))));
        }
    }
    let statement = client.prepare("SELECT * FROM v").await.unwrap();
    s.execute("DROP VIEW v",&[],None).unwrap();
    let refresh_error = client.query(&statement,&[]).await.unwrap_err();
    let fresh_error = client.prepare("SELECT * FROM v").await.unwrap_err();
    // E939 authorization-before-classification; PG 18.6 gives 42P01 here.
    // This is a deliberate divergence that matches GigglesDB fresh Parse.
    assert_eq!(fresh_error.code().unwrap().code(), "42501");
    assert_eq!(refresh_error.code(), fresh_error.code());
    drop(client); connection.await.unwrap().unwrap(); running.shutdown().await.unwrap();
    drop(s); drop(service); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
