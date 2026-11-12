use giggles_core::{Admin, Engine, Limits, roles::{NewRole, SchemaPrivilege}};
use giggles_server::{Service, auth::{PostgresqlConfig, TlsMode, serve_postgresql}, clock::Clock};
use std::{collections::BTreeMap, sync::Arc};

#[tokio::test]
async fn comment_ownership_precedes_semantics_on_simple_and_extended_wire() {
    let path = std::env::temp_dir().join(format!("comments-network-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let engine = if cfg!(feature = "engine-rocksdb") { Engine::V2 } else { Engine::Legacy };
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    for name in ["owner", "other", "root"] {
        admin.roles().create(NewRole { name: name.into(), login: true, superuser: name == "root", ..Default::default() }).unwrap();
        admin.roles().grant_schema("public", name, &[SchemaPrivilege::Create, SchemaPrivilege::Usage]).unwrap();
    }
    let service = Service::new(admin, vec![], Clock::new(0,3600).unwrap()).unwrap();
    service.role_session("owner").unwrap().execute_sql("CREATE TABLE t(id int)",&[]).unwrap();
    let config = PostgresqlConfig { listen: "127.0.0.1:0".parse().unwrap(), tls: TlsMode::Disabled,
        certificate: None, private_key: None, client_ca: None, certificates: BTreeMap::new(),
        databases: vec!["postgres".into()], loopback_trust: true, connections: 8 };
    let running = serve_postgresql(config.listen, service.clone(), Arc::new(config.build().unwrap())).await.unwrap();
    for user in ["owner", "other", "root"] {
        let (client, connection) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port())
            .user(user).dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
        let connection = tokio::spawn(connection);
        for (sql, state) in [("COMMENT ON TABLE t IS 'x'", ""), ("COMMENT ON COLUMN t.absent IS 'x'", "42703"),
            ("COMMENT ON TABLE absent IS 'x'", "42P01")] {
            let expected = if user == "other" || (user == "owner" && sql.contains("TABLE absent")) { "42501" }
                else if engine == Engine::Legacy { "0A000" } else { state };
            for extended in [false, true] {
                let error = if extended { client.execute(sql,&[]).await.map(|_| ()).err() }
                    else { client.simple_query(sql).await.map(|_| ()).err() };
                assert_eq!(error.as_ref().and_then(|e| e.code()).map(|c| c.code()).unwrap_or(""), expected, "{user}: {sql}");
            }
        }
        drop(client); connection.await.unwrap().unwrap();
    }
    running.shutdown().await.unwrap(); drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
