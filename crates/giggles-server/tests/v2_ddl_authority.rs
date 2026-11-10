#![cfg(feature = "engine-rocksdb")]
use giggles_core::{
    Admin, Engine, Limits,
    roles::{NewRole, SchemaPrivilege, TablePrivilege},
};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, mpsc},
    time::Duration,
};
use tokio_postgres::NoTls;

#[test]
fn prepared_ddl_ownership_commits_and_rolls_back_without_deadlock() {
    let (done, completed) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let path = std::env::temp_dir().join(format!("v2-ddl-owner-{}", std::process::id()));
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let roles = admin.roles();
        roles
            .create(NewRole {
                name: "owner".into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        roles
            .grant_schema(
                "public",
                "owner",
                &[SchemaPrivilege::Create, SchemaPrivilege::Usage],
            )
            .unwrap();
        let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let config = PostgresqlConfig {
                listen: "127.0.0.1:0".parse().unwrap(),
                tls: TlsMode::Disabled,
                certificate: None,
                private_key: None,
                client_ca: None,
                certificates: BTreeMap::new(),
                databases: vec!["postgres".into()],
                loopback_trust: true,
                connections: 4,
            };
            let pg = serve_postgresql(
                config.listen,
                service.clone(),
                Arc::new(config.build().unwrap()),
            )
            .await
            .unwrap();
            let (client, connection) = tokio_postgres::Config::new()
                .host("127.0.0.1")
                .port(pg.address.port())
                .user("owner")
                .dbname("postgres")
                .connect(NoTls)
                .await
                .unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            // execute uses Parse/Bind/Execute, including inside an explicit block.
            for sql in [
                "BEGIN",
                "CREATE TABLE created (id BIGINT PRIMARY KEY)",
                "INSERT INTO created VALUES (1)",
            ] {
                client.execute(sql, &[]).await.unwrap();
            }
            assert_eq!(roles.table_owner("created").unwrap(), None);
            client.execute("COMMIT", &[]).await.unwrap();
            assert_eq!(
                roles.table_owner("created").unwrap().as_deref(),
                Some("owner")
            );
            roles
                .grant_table("created", "owner", &[TablePrivilege::Select])
                .unwrap();
            // Wire DROP currently uses an administrative route that forbids
            // explicit blocks; exercise transactional DROP through Service.
            let mut dropper = service.role_session("owner").unwrap();
            dropper.execute_sql("BEGIN", &[]).unwrap();
            dropper.execute_sql("DROP TABLE created", &[]).unwrap();
            assert_eq!(
                roles.table_owner("created").unwrap().as_deref(),
                Some("owner")
            );
            dropper.execute_sql("ROLLBACK", &[]).unwrap();
            assert!(admin.sql_session().relation_exists("created").unwrap());
            assert!(
                roles
                    .table_privileges("created", "owner")
                    .unwrap()
                    .contains(&TablePrivilege::Select)
            );
            client.execute("BEGIN", &[]).await.unwrap();
            client
                .execute("CREATE TABLE discarded (id BIGINT PRIMARY KEY)", &[])
                .await
                .unwrap();
            client.execute("ROLLBACK", &[]).await.unwrap();
            assert_eq!(roles.table_owner("discarded").unwrap(), None);
            client.execute("DROP TABLE created", &[]).await.unwrap();
            assert_eq!(roles.table_owner("created").unwrap(), None);
            assert!(
                roles
                    .table_privileges("created", "owner")
                    .unwrap()
                    .is_empty()
            );
            drop(dropper);
            pg.shutdown().await.unwrap();
        });
        runtime.shutdown_timeout(Duration::from_secs(1));
        drop(service);
        drop(roles);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
        done.send(()).unwrap();
    });
    let result = completed.recv_timeout(Duration::from_secs(15));
    assert!(result.is_ok(), "extended DDL did not finish: {result:?}");
    worker.join().unwrap();
}
