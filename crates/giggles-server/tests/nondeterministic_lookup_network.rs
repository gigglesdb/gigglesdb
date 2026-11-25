use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

#[test]
fn nondeterministic_keys_over_simple_and_extended_pgwire() {
    run_on(Engine::Legacy);
    #[cfg(feature = "engine-rocksdb")]
    run_on(Engine::V2);
}

fn run_on(engine: Engine) {
    let path = std::env::temp_dir().join(format!(
        "nondeterministic-wire-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(check_protocols(&path, engine));
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    std::fs::remove_dir_all(path).unwrap();
}

async fn check_protocols(path: &std::path::Path, engine: Engine) {
    let admin = Admin::create_engine(path, Limits::default(), engine).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "postgres".into(),
            login: true,
            superuser: true,
            ..Default::default()
        })
        .unwrap();
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
    let running = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let (client, connection) = Config::new()
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("postgres")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    for sql in [
        "CREATE COLLATION ci (provider=icu,locale='und-u-ks-level2',deterministic=false)",
        "CREATE TABLE t (id text COLLATE ci PRIMARY KEY, v int)",
        "INSERT INTO t VALUES ('x',1),('y',2),('z',3)",
        "CREATE TABLE c (id int PRIMARY KEY, pid text COLLATE ci REFERENCES t(id))",
        "INSERT INTO c VALUES (1,'X')",
    ] {
        client.simple_query(sql).await.unwrap();
    }
    assert!(matches!(
        client
            .simple_query("UPDATE t SET v=v+1 WHERE id='X'")
            .await
            .unwrap()
            .as_slice(),
        [SimpleQueryMessage::CommandComplete(1)],
    ));
    assert_eq!(
        client
            .execute("UPDATE t SET v=10 WHERE id=$1", &[&"X"])
            .await
            .unwrap(),
        1
    );
    let selected = client
        .query_one("SELECT v FROM t WHERE id=$1", &[&"X"])
        .await
        .unwrap();
    assert_eq!(selected.get::<_, i32>(0), 10);
    let deleted = client
        .query_one("DELETE FROM t WHERE id=$1 RETURNING id", &[&"Y"])
        .await
        .unwrap();
    assert_eq!(deleted.get::<_, &str>(0), "y");
    assert!(matches!(
        client
            .simple_query("DELETE FROM t WHERE id='Z'")
            .await
            .unwrap()
            .as_slice(),
        [SimpleQueryMessage::CommandComplete(1)],
    ));
    let like = client
        .simple_query("SELECT id FROM t WHERE id LIKE 'X%' COLLATE ci")
        .await
        .unwrap();
    assert!(
        matches!(like.as_slice(), [SimpleQueryMessage::RowDescription(_), SimpleQueryMessage::Row(row),
        SimpleQueryMessage::CommandComplete(1)] if row.get(0) == Some("x"))
    );
    let like = client
        .query_one("SELECT id FROM t WHERE id ~~ ($1 COLLATE ci)", &[&"X%"])
        .await
        .unwrap();
    assert_eq!(like.get::<_, &str>(0), "x");

    let simple = client
        .simple_query("DELETE FROM t WHERE id='X'")
        .await
        .unwrap_err();
    let extended = client
        .execute("UPDATE t SET id='zzz' WHERE id=$1", &[&"X"])
        .await
        .unwrap_err();
    for error in [simple, extended] {
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "23503");
        assert_eq!(
            error.message(),
            "update or delete on table \"t\" violates foreign key constraint \"c_pid_fkey\" on table \"c\""
        );
        assert_eq!(
            error.detail(),
            Some("Key (id)=(x) is still referenced from table \"c\".")
        );
        assert_eq!(error.table(), Some("c"));
        assert_eq!(error.schema(), Some("public"));
        assert_eq!(error.constraint(), Some("c_pid_fkey"));
    }
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
}
