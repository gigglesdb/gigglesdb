#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
#[test]
fn column_collation_wire_metadata_and_errors() {
    let path = std::env::temp_dir().join(format!(
        "column-collation-wire-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let service = runtime.block_on(check_column_collation(&path));
    // Listener shutdown aborts socket tasks, but started spawn_blocking SQL
    // work may still own a session. Runtime shutdown waits for those workers.
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    assert!(service.upgrade().is_none(), "SQL worker retained the service");
    std::fs::remove_dir_all(path).unwrap();
}

async fn check_column_collation(path: &std::path::Path) -> std::sync::Weak<Service> {
    let admin = Admin::create_engine(path, Limits::default(), Engine::V2).unwrap();
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
    let stopped_service = Arc::downgrade(&service);
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
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("postgres")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    client.batch_execute(r#"CREATE COLLATION ci(provider=icu,locale='und-u-ks-level2',deterministic=false);CREATE TABLE c1(id int PRIMARY KEY,a text COLLATE "und-x-icu",b text COLLATE "C",c text COLLATE ci,d varchar(10) COLLATE "de-x-icu",e text);INSERT INTO c1 VALUES(1,'b','b','ABC','a','x'),(2,'B','B','abc','a','y'),(3,'a','a','Abc','z','z')"#).await.unwrap();
    let rows=client.query("SELECT a.attname,a.attcollation::regcollation::text FROM pg_attribute a JOIN pg_class t ON t.oid=a.attrelid WHERE t.relname='c1' AND a.attnum>0 ORDER BY a.attnum",&[]).await.unwrap();
    let cols = rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        cols,
        vec![
            ("id".into(), "-".into()),
            ("a".into(), "\"und-x-icu\"".into()),
            ("b".into(), "\"C\"".into()),
            ("c".into(), "ci".into()),
            ("d".into(), "\"de-x-icu\"".into()),
            ("e".into(), "\"default\"".into())
        ]
    );
    let rows = client
        .query("SELECT id FROM c1 WHERE c='abc' ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    for sql in [
        "SELECT min(a||b) FROM c1",
        "SELECT max(a||b) FROM c1",
        "SELECT greatest(a,b) FROM c1",
        "SELECT least(a,b) FROM c1",
    ] {
        let prepared = client.prepare(sql).await.unwrap();
        let error = client.query(&prepared, &[]).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "42P22", "{sql}");
        assert_eq!(error.as_db_error().unwrap().message(),
            "could not determine which collation to use for string comparison");
    }
    // PostgreSQL assigns aggregate sort-key collations during planning,
    // including empty and singleton inputs; plain MIN/MAX stay lazy below.
    for sql in [
        "SELECT max(a ORDER BY a||b) FROM c1",
        "SELECT max(a ORDER BY a||b) FROM c1 WHERE id=-1",
        "SELECT max(a||b ORDER BY a||b) FROM c1 WHERE id=1",
        "SELECT count(DISTINCT a||b) FROM c1 WHERE id=-1",
    ] {
        let error = client.prepare(sql).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "42P21", "{sql}");
    }
    for sql in [
        "SELECT min(a||b) FROM c1 WHERE id=1",
        "SELECT max(a||b) FROM c1 WHERE id=1",
        "SELECT greatest(a||b) FROM c1 WHERE id=1",
        "SELECT least(a||b,NULL) FROM c1 WHERE id=1",
    ] {
        let prepared = client.prepare(sql).await.unwrap();
        let rows = client.query(&prepared,&[]).await.unwrap();
        assert_eq!(rows[0].get::<_,String>(0), "bb", "{sql}");
    }
    for sql in ["SELECT a=b FROM c1", "SELECT a<b FROM c1"] {
        let e = client.query(sql, &[]).await.unwrap_err();
        assert_eq!(e.code().unwrap().code(), "42P22");
        assert_eq!(
            e.as_db_error().unwrap().message(),
            "could not determine which collation to use for string comparison"
        );
    }
    let e = client
        .query("SELECT a||b FROM c1 ORDER BY 1", &[])
        .await
        .unwrap_err();
    assert_eq!(e.code().unwrap().code(), "42P21");
    client
        .batch_execute("CREATE TABLE c5 AS SELECT a,b FROM c1")
        .await
        .unwrap();
    assert_eq!(
        client
            .query("SELECT a FROM c5 ORDER BY a", &[])
            .await
            .unwrap()
            .len(),
        3
    );
    for (corpus, sql) in [
        include_str!("../../../compat/corpus/pg_types/pgy_collate_c.sql"),
        include_str!("../../../compat/corpus/pg_types/pgy_collate_posix.sql"),
        include_str!("../../../compat/corpus/pg_types/pgy_collate_unicode.sql"),
        include_str!("../../../compat/corpus/pg_types/pgy_collation_icu.sql"),
        include_str!("../../../compat/corpus/pg_silent/pgs_collation.sql"),
        include_str!("../../../compat/corpus/pg_ddl/pgd_comment_collation.sql"),
        include_str!("../../../compat/corpus/pg_types/pgy_collation_lifecycle.sql"),
    ].into_iter().enumerate() {
        // These fixtures use full-line comments and no quoted semicolons.
        // Send each statement separately, matching the psql -f corpus harness.
        let body=sql.lines().filter(|line|!line.trim_start().starts_with("--")).collect::<Vec<_>>().join("\n");
        for (statement, sql) in body.split(';').filter(|sql|!sql.trim().is_empty()).enumerate() {
            client.batch_execute(sql).await.unwrap_or_else(|e|panic!("corpus {corpus}, statement {statement}: {e}"));
        }
    }
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    stopped_service
}
