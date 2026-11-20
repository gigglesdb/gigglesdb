use giggles_core::{Admin, Engine, Limits, roles::{NewRole, SchemaPrivilege}};
use giggles_server::{Service, auth::{PostgresqlConfig, TlsMode, serve_postgresql}, clock::Clock};
use std::{collections::BTreeMap, sync::Arc};

#[tokio::test]
async fn view_ddl_authorization_precedes_semantics() {
    let path = std::env::temp_dir().join(format!("view-wire-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let engine = if cfg!(feature = "engine-rocksdb") { Engine::V2 } else { Engine::Legacy };
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    for name in ["owner", "other"] {
        admin.roles().create(NewRole { name: name.into(), login: true, ..Default::default() }).unwrap();
        // The denied role lacks target-schema CREATE. Source SELECT is deferred
        // until a view read, so it cannot be the DDL authorization boundary.
        let privileges = if name == "owner" { &[SchemaPrivilege::Create, SchemaPrivilege::Usage][..] }
            else { &[SchemaPrivilege::Usage][..] };
        admin.roles().grant_schema("public", name, privileges).unwrap();
    }
    let service = Service::new(admin.clone(), vec![], Clock::new(0,3600).unwrap()).unwrap();
    service.role_session("owner").unwrap().execute_sql("CREATE TABLE t(id int)",&[]).unwrap();
    let config = PostgresqlConfig { listen: "127.0.0.1:0".parse().unwrap(), tls: TlsMode::Disabled,
        certificate: None, private_key: None, client_ca: None, certificates: BTreeMap::new(),
        databases: vec!["postgres".into()], loopback_trust: true, connections: 8 };
    let running = serve_postgresql(config.listen, service.clone(), Arc::new(config.build().unwrap())).await.unwrap();
    for user in ["owner", "other"] {
        let (client, connection) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port())
            .user(user).dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
        let connection = tokio::spawn(connection);
        for extended in [false, true] {
            for (sql, expected) in [("CREATE VIEW v AS SELECT * FROM t", ""), ("DROP VIEW v", ""),
                ("CREATE VIEW v AS SELECT id+1 FROM t", "0A000")] {
                let sql = if engine == Engine::Legacy && sql == "DROP VIEW v" { "DROP VIEW t" } else { sql };
                let expected = if user == "other" { "42501" } else if engine == Engine::Legacy { "0A000" } else { expected };
                let error = if extended { client.execute(sql,&[]).await.map(|_| ()).err() } else { client.simple_query(sql).await.map(|_| ()).err() };
                assert_eq!(error.as_ref().and_then(|e| e.code()).map(|c| c.code()).unwrap_or(""), expected, "{user}: {sql}");
            }
        }
        if user == "owner" {
            if engine == Engine::V2 { client.simple_query("CREATE VIEW ro AS SELECT id FROM t").await.unwrap(); }
            for extended in [false, true] {
                for sql in ["CREATE VIEW ro AS SELECT id FROM t", "DROP VIEW IF EXISTS ro", "COMMENT ON TABLE t IS 'ro'", "ALTER TABLE t ADD COLUMN ro int"] {
                    let sql = if engine == Engine::Legacy && sql.starts_with("DROP VIEW") { "DROP VIEW t" } else { sql };
                    client.simple_query("BEGIN READ ONLY").await.unwrap();
                    let error = if extended { client.execute(sql, &[]).await.unwrap_err() } else { client.simple_query(sql).await.unwrap_err() };
                    assert_eq!(error.code().unwrap().code(), "25006", "{sql}");
                    client.simple_query("ROLLBACK").await.unwrap();
                }
                if engine == Engine::V2 {
                    client.simple_query("CREATE OR REPLACE VIEW v(x) AS SELECT id FROM t").await.unwrap();
                    for (sql, code, message) in [("CREATE OR REPLACE VIEW v AS SELECT id FROM t", "42P16", "cannot change name of view column"),
                        ("CREATE OR REPLACE VIEW t AS SELECT id FROM t", "42809", "")] {
                        let error = if extended { client.execute(sql, &[]).await.unwrap_err() } else { client.simple_query(sql).await.unwrap_err() };
                        assert_eq!(error.code().unwrap().code(), code);
                        assert!(error.as_db_error().unwrap().message().starts_with(message));
                    }
                }
            }
        } else if engine == Engine::V2 {
            admin.roles().grant_table("t", "other", &[giggles_core::roles::TablePrivilege::Select]).unwrap();
            for extended in [false, true] {
                let sql = "CREATE OR REPLACE VIEW v(x) AS SELECT id FROM t";
                let error = if extended { client.execute(sql, &[]).await.unwrap_err() } else { client.simple_query(sql).await.unwrap_err() };
                assert_eq!(error.code().unwrap().code(), "42501");
            }
        }
        drop(client); connection.await.unwrap().unwrap();
    }
    running.shutdown().await.unwrap(); drop(service); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
