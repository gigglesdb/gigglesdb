#![cfg(feature = "engine-rocksdb")]

use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{path::Path, sync::Arc, time::SystemTime};
use tokio_postgres::{NoTls, binary_copy::BinaryCopyInWriter, error::SqlState, types::Type};

fn assert_duplicate(error: &tokio_postgres::Error, table: &str, constraint: &str, detail: &str) {
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code(), &SqlState::UNIQUE_VIOLATION);
    assert_eq!(
        error.message(),
        format!("duplicate key value violates unique constraint \"{constraint}\"")
    );
    assert_eq!(error.constraint(), Some(constraint));
    assert_eq!(error.table(), Some(table));
    assert_eq!(error.schema(), Some("public"));
    assert_eq!(error.detail(), Some(detail));
}

#[tokio::test]
async fn unique_reporting_simple_and_parameterized() {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "unique-reporting-network-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create_engine(path.join("db"), Limits::default(), Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..NewRole::default()
        })
        .unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: Default::default(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 4,
    };
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let server = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(server.address.ip())
        .port(server.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);

    for sql in [
        "CREATE TABLE a (id int PRIMARY KEY, e text, del boolean)",
        "CREATE UNIQUE INDEX a_e_live ON a(e) WHERE NOT del",
        "INSERT INTO a VALUES (1, 'x', false)",
    ] {
        client.batch_execute(sql).await.unwrap();
    }

    for (id, tail, constraint, detail) in [
        (1_i32, "", "a_pkey", "Key (id)=(1) already exists."),
        (
            2_i32,
            " ON CONFLICT (id) DO NOTHING",
            "a_e_live",
            "Key (e)=(x) already exists.",
        ),
    ] {
        let extended = format!("INSERT INTO a VALUES ($1, $2, $3){tail}");
        let simple = format!("INSERT INTO a VALUES ({id}, 'x', false){tail}");
        for error in [
            client
                .execute(&extended, &[&id, &"x", &false])
                .await
                .unwrap_err(),
            client.simple_query(&simple).await.unwrap_err(),
        ] {
            assert_duplicate(&error, "a", constraint, detail);
        }
    }

    let copy = client
        .copy_in("COPY a FROM STDIN (FORMAT BINARY)")
        .await
        .unwrap();
    let mut copy = std::pin::pin!(BinaryCopyInWriter::new(
        copy,
        &[Type::INT4, Type::TEXT, Type::BOOL]
    ));
    copy.as_mut().write(&[&7_i32, &"q", &false]).await.unwrap();
    copy.as_mut().write(&[&1_i32, &"x", &false]).await.unwrap();
    let error = copy.as_mut().finish().await.unwrap_err();
    assert_duplicate(&error, "a", "a_pkey", "Key (id)=(1) already exists.");
    let count: i64 = client
        .query_one("SELECT count(*) FROM a", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);

    client
        .batch_execute(
            "CREATE TABLE bb (id int, e text, del boolean);
         CREATE UNIQUE INDEX bb_e_live ON bb(e) WHERE NOT del;
         ALTER TABLE bb ADD PRIMARY KEY (id);
         INSERT INTO bb VALUES (1,'x',false),(2,'y',false);
         CREATE TABLE dd (id int, u int);
         CREATE UNIQUE INDEX dd_u ON dd(u);
         ALTER TABLE dd ADD PRIMARY KEY (id);
         INSERT INTO dd VALUES (1,1),(2,2);",
        )
        .await
        .unwrap();

    for (simple, extended) in [
        (
            "ALTER INDEX dd_u RENAME TO bb",
            "ALTER INDEX public.dd_u RENAME TO bb",
        ),
        (
            "ALTER INDEX dd_u RENAME TO dd_u",
            "ALTER INDEX public.dd_u RENAME TO dd_u",
        ),
        (
            "ALTER INDEX dd_u RENAME TO dd_pkey",
            "ALTER INDEX public.dd_u RENAME TO dd_pkey",
        ),
    ] {
        let name = simple.split_whitespace().last().unwrap();
        for error in [
            client.simple_query(simple).await.unwrap_err(),
            client.execute(extended, &[]).await.unwrap_err(),
        ] {
            let error = error.as_db_error().unwrap();
            assert_eq!(error.code(), &SqlState::DUPLICATE_TABLE, "{simple}");
            assert_eq!(
                error.message(),
                format!("relation \"{name}\" already exists")
            );
            assert_eq!(error.constraint(), None);
            assert_eq!(error.table(), None);
            assert_eq!(error.schema(), None);
            // The wire adds its generic phase diagnostic to DDL failures.
            assert!(
                error
                    .detail()
                    .is_none_or(|detail| detail.starts_with("phase="))
            );
        }
    }
    for error in [
        client
            .simple_query("UPDATE bb SET id=1, e='x' WHERE id=2")
            .await
            .unwrap_err(),
        client
            .execute(
                "UPDATE bb SET id=$1, e=$2 WHERE id=$3",
                &[&1_i32, &"x", &2_i32],
            )
            .await
            .unwrap_err(),
    ] {
        assert_duplicate(&error, "bb", "bb_e_live", "Key (e)=(x) already exists.");
    }

    for (simple, extended, right) in [
        (
            "UPDATE dd SET u=1 WHERE id=2",
            "UPDATE dd SET u=$1 WHERE id=$2",
            2_i32,
        ),
        (
            "INSERT INTO dd SELECT 1,1",
            "INSERT INTO dd SELECT $1::int,$2::int",
            1,
        ),
        (
            "WITH x AS (SELECT 1 id, 1 u) INSERT INTO dd SELECT * FROM x",
            "WITH x AS (SELECT $1::int id, $2::int u) INSERT INTO dd SELECT * FROM x",
            1,
        ),
        (
            "INSERT INTO dd SELECT 3,1 ON CONFLICT (id) DO NOTHING",
            "INSERT INTO dd SELECT $2::int,$1::int ON CONFLICT (id) DO NOTHING",
            3,
        ),
    ] {
        for error in [
            client.simple_query(simple).await.unwrap_err(),
            client
                .execute(extended, &[&1_i32, &right])
                .await
                .unwrap_err(),
        ] {
            assert_duplicate(&error, "dd", "dd_u", "Key (u)=(1) already exists.");
        }
    }

    client
        .batch_execute(
            "CREATE TABLE ff (u int);
         CREATE INDEX ON ff(u);
         CREATE INDEX ON ff(u);
         CREATE TABLE t_u_key (id int);
         CREATE TABLE t (u int UNIQUE);
         ALTER TABLE t ADD UNIQUE(u);
         INSERT INTO t VALUES (1);",
        )
        .await
        .unwrap();
    client.execute("CREATE INDEX ON ff(u)", &[]).await.unwrap();
    let names = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE tablename IN ('ff', 't') ORDER BY indexname",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["ff_u_idx", "ff_u_idx1", "ff_u_idx2", "t_u_key1", "t_u_key2"]
    );
    for error in [
        client
            .simple_query("INSERT INTO t VALUES (1)")
            .await
            .unwrap_err(),
        client
            .execute("INSERT INTO t VALUES ($1)", &[&1_i32])
            .await
            .unwrap_err(),
    ] {
        assert_duplicate(&error, "t", "t_u_key1", "Key (u)=(1) already exists.");
    }

    drop(client);
    connection.await.unwrap().unwrap();
    server.shutdown().await.unwrap();
}
