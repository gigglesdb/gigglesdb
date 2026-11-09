use giggles_core::{
    Admin, Limits, Principal, TaskId,
    roles::{NewRole, SchemaPrivilege},
};
use giggles_server::{
    Binding, Role, Service,
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
    clock::Clock,
    sqlstate,
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Config, NoTls};

#[tokio::test]
async fn constraint_wire_errors_and_deferred_commit_rollback() {
    let root = std::path::PathBuf::from(std::env::var_os("TEMP").expect("task TEMP"));
    assert!(
        root.to_string_lossy()
            .replace('\\', "/")
            .to_ascii_lowercase()
            .starts_with("j:/gigglesdb-work/")
    );
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = root.join(format!(
        "constraints-network-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
    for name in ["owner", "other"] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                login: true,
                password: Some(Verifier::generate("pw")),
                ..NewRole::default()
            })
            .unwrap();
        admin
            .roles()
            .grant_schema(
                "public",
                name,
                &[SchemaPrivilege::Create, SchemaPrivilege::Usage],
            )
            .unwrap();
    }
    let fingerprint = "c".repeat(64);
    let service = Service::new(
        admin.clone(),
        vec![Binding {
            fingerprint: fingerprint.clone(),
            role: Role::Task {
                domain: admin.authority_metadata().unwrap().0.to_string(),
                task: TaskId(91),
                principal: Principal(92),
            },
        }],
        Clock::new(0, 3600).unwrap(),
    )
    .unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: BTreeMap::new(),
        databases: vec!["postgres".into()],
        loopback_trust: false,
        connections: 8,
    };
    let listener = config.build().unwrap();
    let running = serve_postgresql(config.listen, service.clone(), Arc::new(listener))
        .await
        .unwrap();
    let mut config = Config::new();
    config
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("owner")
        .password("pw")
        .dbname("postgres");
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    for extended in [false, true] {
        let suffix = if extended { "extended" } else { "simple" };
        let parent = format!("parent_{suffix}");
        let child = format!("child_{suffix}");
        let heap = format!("dump_heap_{suffix}");
        for sql in [
            format!("CREATE TABLE {heap}(id INT,n INT)"),
            format!("INSERT INTO {heap} VALUES(1,10),(2,20)"),
            format!("ALTER TABLE ONLY public.{heap} ADD CONSTRAINT {heap}_pkey PRIMARY KEY(id)"),
        ] {
            if extended {
                client.execute(&sql, &[]).await.unwrap();
            } else {
                client.simple_query(&sql).await.unwrap();
            }
        }
        let error = client
            .execute(&format!("INSERT INTO {heap} VALUES(1,30)"), &[])
            .await
            .unwrap_err();
        assert_eq!(error.code().unwrap().code(), "23505");
        let error = client
            .execute(&format!("INSERT INTO {heap} VALUES(NULL,30)"), &[])
            .await
            .unwrap_err();
        assert_eq!(error.code().unwrap().code(), "23502");
        let statements = [
            format!("CREATE TABLE {parent} (id BIGINT PRIMARY KEY)"),
            format!(
                "CREATE TABLE {child} (id BIGINT PRIMARY KEY, parent_id BIGINT, CONSTRAINT child_fk_{suffix} FOREIGN KEY (parent_id) REFERENCES {parent} (id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED)"
            ),
            "BEGIN".into(),
            format!("INSERT INTO {child} VALUES (1, 99)"),
        ];
        for sql in statements {
            if extended {
                client.execute(&sql, &[]).await.unwrap();
            } else {
                client.simple_query(&sql).await.unwrap();
            }
        }
        let error = if extended {
            client.execute("COMMIT", &[]).await.unwrap_err()
        } else {
            client.simple_query("COMMIT").await.unwrap_err()
        };
        assert_eq!(error.code().unwrap().code(), "23503");
        assert!(
            client
                .query(&format!("SELECT id FROM {child} LIMIT 10"), &[])
                .await
                .unwrap()
                .is_empty()
        );
        client
            .simple_query(&format!(
                "INSERT INTO {parent} VALUES (1); INSERT INTO {child} VALUES (1, 1)"
            ))
            .await
            .unwrap();
        let error = if extended {
            client
                .execute(&format!("DELETE FROM {parent} WHERE id = 1"), &[])
                .await
                .unwrap_err()
        } else {
            client
                .simple_query(&format!("DELETE FROM {parent} WHERE id = 1"))
                .await
                .unwrap_err()
        };
        assert_eq!(error.code().unwrap().code(), "23001");
    }
    let mut owner = service.role_session("owner").unwrap();
    let mut other = service.role_session("other").unwrap();
    let mut task = service.session(&fingerprint).unwrap();
    other
        .execute_sql("CREATE TABLE foreign_owned (id BIGINT PRIMARY KEY)", &[])
        .unwrap();
    for sql in [
        "ALTER TABLE parent_simple ADD CHECK (missing > 0)",
        "ALTER TABLE parent_simple ADD PRIMARY KEY (missing)",
        "ALTER TABLE parent_simple VALIDATE CONSTRAINT missing",
        "ALTER TABLE parent_simple DROP CONSTRAINT missing",
        "ALTER TABLE absent ADD CHECK (missing > 0)",
        "ALTER TABLE parent_simple ADD CONSTRAINT hidden_fk FOREIGN KEY (missing) REFERENCES hidden.missing (missing)",
        "DROP TABLE parent_simple",
        "DROP TABLE absent",
        "DROP TABLE IF EXISTS absent CASCADE",
        "DROP TABLE hidden.missing",
        "DROP TABLE giggles_reserved",
    ] {
        for session in [&mut other, &mut task] {
            assert_eq!(
                sqlstate(&session.execute_sql(sql, &[]).unwrap_err()),
                "42501",
                "{sql}"
            );
        }
    }
    // Referenced ownership must precede column and key resolution, including CREATE.
    for sql in [
        "ALTER TABLE parent_simple ADD CONSTRAINT denied_fk FOREIGN KEY (missing) REFERENCES foreign_owned (missing)",
        "ALTER TABLE parent_simple ADD CONSTRAINT denied_fk FOREIGN KEY (missing) REFERENCES absent (missing)",
        "CREATE TABLE denied_child (id BIGINT REFERENCES foreign_owned (missing))",
        "ALTER TABLE parent_simple ADD CONSTRAINT hidden_fk FOREIGN KEY (missing) REFERENCES hidden.missing (missing)",
        "CREATE TABLE hidden_child (id BIGINT REFERENCES hidden.missing (missing))",
    ] {
        assert_eq!(
            sqlstate(&owner.execute_sql(sql, &[]).unwrap_err()),
            "42501",
            "{sql}"
        );
        assert_eq!(
            sqlstate(&task.execute_sql(sql, &[]).unwrap_err()),
            "42501",
            "{sql}"
        );
    }
    // A new relation may reference itself without a preexisting owner record.
    owner.execute_sql(
        "CREATE TABLE self_child (id BIGINT PRIMARY KEY, parent_id BIGINT REFERENCES self_child (id))",
        &[],
    ).unwrap();
    owner
        .execute_sql("INSERT INTO self_child VALUES (1, 1)", &[])
        .unwrap();
    owner
        .execute_sql("CREATE TABLE drop_parent(id INT PRIMARY KEY)", &[])
        .unwrap();
    owner
        .execute_sql(
            "CREATE TABLE drop_child(id INT PRIMARY KEY,pid INT REFERENCES drop_parent(id))",
            &[],
        )
        .unwrap();
    owner
        .execute_sql("DROP TABLE drop_parent CASCADE", &[])
        .unwrap();
    owner
        .execute_sql("INSERT INTO drop_child VALUES(1,999)", &[])
        .unwrap();
    assert_eq!(
        owner
            .execute_sql("SELECT pid FROM drop_child LIMIT 10", &[])
            .unwrap()
            .rows[0]
            .values,
        vec![giggles_core::relational::Value::Int(999)]
    );
    // RI actions run with constraint-owner authority, without child DML grants.
    owner
        .execute_sql("CREATE TABLE cascade_parent(id INT PRIMARY KEY)", &[])
        .unwrap();
    owner.execute_sql("CREATE TABLE cascade_child(id INT PRIMARY KEY,pid INT REFERENCES cascade_parent(id) ON UPDATE CASCADE ON DELETE CASCADE)", &[]).unwrap();
    owner
        .execute_sql("INSERT INTO cascade_parent VALUES(1)", &[])
        .unwrap();
    owner
        .execute_sql("INSERT INTO cascade_child VALUES(1,1)", &[])
        .unwrap();
    admin
        .roles()
        .set_table_owner("cascade_child", "other")
        .unwrap();
    owner
        .execute_sql("UPDATE cascade_parent SET id=2 WHERE id=1", &[])
        .unwrap();
    let result = other
        .execute_sql("SELECT pid FROM cascade_child LIMIT 10", &[])
        .unwrap();
    assert_eq!(
        result.rows[0].values,
        vec![giggles_core::relational::Value::Int(2)]
    );
    owner
        .execute_sql("DELETE FROM cascade_parent WHERE id=2", &[])
        .unwrap();
    assert!(
        other
            .execute_sql("SELECT pid FROM cascade_child LIMIT 10", &[])
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(sqlstate(&task.execute_sql(
        "CREATE TABLE denied_self_child (id BIGINT PRIMARY KEY REFERENCES denied_self_child (id))",
        &[],
    ).unwrap_err()), "42501");
    let prepared = owner
        .prepare("ALTER TABLE parent_simple ADD CONSTRAINT prepared_fk FOREIGN KEY (id) REFERENCES parent_extended (id)")
        .unwrap();
    admin
        .roles()
        .set_table_owner("parent_extended", "other")
        .unwrap();
    assert_eq!(
        sqlstate(&owner.execute_prepared(&prepared, &[]).unwrap_err()),
        "42501"
    );
    drop(task);
    drop(other);
    drop(owner);
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(service);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
