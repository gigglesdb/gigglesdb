use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Config, NoTls};

async fn start_test_database(engine: Engine, name: &str) -> (PathBuf, Admin, Running) {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "volatile-uuid-network-{name}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            superuser: true,
            login: true,
            ..NewRole::default()
        })
        .unwrap();
    let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
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
    let listener = config.build().unwrap();
    let running = serve_postgresql(config.listen, service, Arc::new(listener))
        .await
        .unwrap();
    (path, admin, running)
}

#[tokio::test]
async fn prisma_defaults_indexes_and_diagnostics_over_extended_protocol() {
    let engine = if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    };
    let (path, _, running) = start_test_database(engine, "prisma").await;
    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(running.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    client
        .batch_execute(
            r#"CREATE TABLE "User" (
        "id" uuid NOT NULL DEFAULT gen_random_uuid(),
        "email" text NOT NULL, "orgId" uuid, "name" text,
        CONSTRAINT "User_pkey" PRIMARY KEY ("id"));
        CREATE UNIQUE INDEX "User_email_key" ON "User" ("email");
        CREATE INDEX "User_orgId_idx" ON "User" ("orgId");
        CREATE UNIQUE INDEX "User_orgId_name_key" ON "User" ("orgId", "name");"#,
        )
        .await
        .unwrap();
    let insert = client
        .prepare(r#"INSERT INTO "User" ("email","name") VALUES ($1,$2)"#)
        .await
        .unwrap();
    for (email, name) in [("a@x", "Alice"), ("b@x", "Bob"), ("c@x", "Charlie")] {
        assert_eq!(client.execute(&insert, &[&email, &name]).await.unwrap(), 1);
    }
    let select = client
        .prepare(r#"SELECT "id"::text,"email" FROM "User" ORDER BY "email""#)
        .await
        .unwrap();
    let rows = client.query(&select, &[]).await.unwrap();
    let ids = rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 3);
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    for (row, expected) in rows.iter().zip(["a@x", "b@x", "c@x"]) {
        assert_eq!(row.get::<_, String>(1), expected);
        let uuid: String = row.get(0);
        assert_eq!(uuid.as_bytes()[14], b'7');
        assert!(b"89ab".contains(&uuid.as_bytes()[19]));
    }
    let error = client
        .execute(&insert, &[&"a@x", &"Duplicate"])
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23505");
    assert_eq!(
        error.message(),
        r#"duplicate key value violates unique constraint "User_email_key""#
    );
    assert_eq!(error.detail(), Some("Key (email)=(a@x) already exists."));
    assert_eq!(error.schema(), Some("public"));
    assert_eq!(error.table(), Some("User"));
    assert_eq!(error.constraint(), Some("User_email_key"));
    let default = client.query_one(
        "SELECT column_default FROM information_schema.columns WHERE table_name='User' AND column_name='id'",
        &[],
    ).await.unwrap();
    assert_eq!(default.get::<_, String>(0), "gen_random_uuid()");
    let expression = client
        .query_one("SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef", &[])
        .await
        .unwrap();
    assert_eq!(expression.get::<_, String>(0), "gen_random_uuid()");
    argument_defaults(&client, engine).await;
    let writer_config = Config::new()
        .host("127.0.0.1")
        .port(running.address.port())
        .user("am_writer")
        .dbname("postgres")
        .clone();
    check_duplicate_visibility(&client, writer_config).await;

    let err = client.prepare("SELECT pg_catalog.uuidv7(1)").await.unwrap_err();
    let err = err.as_db_error().unwrap();
    assert_eq!(err.code().code(), "42883");
    assert_eq!(err.message(), "function pg_catalog.uuidv7(integer) does not exist");
    assert_eq!(
        err.hint(),
        Some("No function matches the given name and argument types. You might need to add explicit type casts.")
    );
    for (arg, expected) in [("1e30", "1e+30"), ("'Infinity'::float8", "Infinity"), ("'NaN'::float8", "NaN")] {
        let err = client.query(&format!("SELECT setseed({arg})"), &[]).await.unwrap_err();
        let err = err.as_db_error().unwrap();
        assert_eq!(err.code().code(), "22023");
        assert_eq!(err.message(), format!("setseed parameter {expected} is out of allowed range [-1,1]"));
    }

    let seed = client
        .prepare("SELECT pg_catalog.setseed($1)")
        .await
        .unwrap();
    assert_eq!(seed.columns()[0].type_().oid(), 2278);
    client.execute(&seed, &[&0.5f64]).await.unwrap();
    let value = client.query_one("SELECT random()", &[]).await.unwrap();
    assert_eq!(value.get::<_, f64>(0), 0.9851677175347999);
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn legacy_transactional_create_preserves_ownership_and_filters_duplicate_detail() {
    let (path, admin, running) = start_test_database(Engine::Legacy, "legacy-create").await;
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(running.address.port())
        .user("owner")
        .dbname("postgres");
    let (owner, owner_connection) = config.connect(NoTls).await.unwrap();
    let owner_connection = tokio::spawn(owner_connection);
    owner.batch_execute("CREATE ROLE creator LOGIN").await.unwrap();
    owner
        .batch_execute("GRANT CREATE ON SCHEMA public TO creator")
        .await
        .unwrap();
    let (creator, creator_connection) = config.user("creator").connect(NoTls).await.unwrap();
    let creator_connection = tokio::spawn(creator_connection);

    creator
        .batch_execute("BEGIN; CREATE TABLE t2 (id int)")
        .await
        .unwrap();
    let error = owner.batch_execute("DROP ROLE creator").await.unwrap_err();
    assert_eq!(error.as_db_error().unwrap().code().code(), "2BP01");
    creator.batch_execute("COMMIT").await.unwrap();

    creator
        .batch_execute("BEGIN; CREATE TABLE t (id serial PRIMARY KEY); COMMIT")
        .await
        .unwrap();
    for relation in ["t", "t_id_seq"] {
        assert_eq!(
            admin.roles().table_owner(relation).unwrap().as_deref(),
            Some("creator"),
        );
    }
    let table_owner = owner
        .query_one("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname = 't'", &[])
        .await
        .unwrap();
    assert_eq!(table_owner.get::<_, &str>(0), "creator");
    creator
        .batch_execute("ALTER SEQUENCE t_id_seq RESTART WITH 20")
        .await
        .unwrap();

    creator
        .batch_execute("BEGIN; CREATE TABLE t3 (id uuid PRIMARY KEY)")
        .await
        .unwrap();
    let insert = "INSERT INTO t3 VALUES ('00000000-0000-0000-0000-000000000001')";
    creator.batch_execute(insert).await.unwrap();
    // A concurrent Legacy CREATE can allocate the same relation ID.
    owner
        .batch_execute("CREATE TABLE concurrent_table (id uuid PRIMARY KEY)")
        .await
        .unwrap();
    owner
        .batch_execute("GRANT INSERT, SELECT ON concurrent_table TO creator")
        .await
        .unwrap();
    let error = creator.batch_execute(insert).await.unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23505");
    assert_eq!(
        error.message(),
        r#"duplicate key value violates unique constraint "t3_pkey""#,
    );
    assert_eq!(error.schema(), Some("public"));
    assert_eq!(error.table(), Some("t3"));
    assert_eq!(error.constraint(), Some("t3_pkey"));
    assert_eq!(error.detail(), None);
    creator.batch_execute("ROLLBACK").await.unwrap();

    drop(creator);
    drop(owner);
    creator_connection.await.unwrap().unwrap();
    owner_connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

async fn argument_defaults(client: &tokio_postgres::Client, engine: Engine) {
    client.batch_execute(
        "CREATE TABLE d (id int PRIMARY KEY, r int DEFAULT random(1,2),
         n numeric DEFAULT random(1.5, 2.25), u uuid DEFAULT uuidv7(interval '1 day'),
         v smallint DEFAULT uuid_extract_version(gen_random_uuid()),
         ts timestamptz DEFAULT uuid_extract_timestamp(uuidv7()));
         INSERT INTO d (id) VALUES (1),(2);
         INSERT INTO d VALUES (3,DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT);",
    ).await.unwrap();

    for (ddl, message) in [
        (r#"CREATE TABLE bad (id uuid DEFAULT "pg_catalog.uuidv7"())"#,
         "function pg_catalog.uuidv7() does not exist"),
        (r#"ALTER TABLE d ALTER COLUMN r SET DEFAULT "pg_catalog.random"(1,2)"#,
         "function pg_catalog.random(integer, integer) does not exist"),
    ] {
        let error = client.batch_execute(ddl).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "42883");
        assert_eq!(error.message(), message);
        assert_eq!(error.hint(), Some("No function matches the given name and argument types. You might need to add explicit type casts."));
    }

    let expected = [
        "random(1.5, 2.25)",
        "random(1, 2)",
        "uuid_extract_timestamp(uuidv7())",
        "uuidv7('1 day'::interval)",
        "uuid_extract_version(gen_random_uuid())",
    ];
    let defaults = client.query(
        "SELECT column_default FROM information_schema.columns
         WHERE table_name='d' AND column_default IS NOT NULL ORDER BY column_name", &[],
    ).await.unwrap();
    assert_eq!(defaults.iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>(), expected);

    let omit_sql = "INSERT INTO d(id) VALUES ($1)";
    let omit = client.prepare(omit_sql).await.unwrap();
    client.execute(&omit, &[&4i32]).await.unwrap();
    client.batch_execute("ALTER TABLE d ALTER COLUMN r SET DEFAULT random(5, 6)").await.unwrap();
    let explicit = client.prepare(
        "INSERT INTO d VALUES ($1,DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT)",
    ).await.unwrap();
    client.execute(&explicit, &[&5i32]).await.unwrap();
    let omit = if engine == Engine::Legacy {
        // Legacy rejects prepared statements from an earlier schema epoch after DDL.
        let error = client.execute(&omit, &[&6i32]).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "0A000");
        assert!(error.detail().is_some_and(|detail| detail.starts_with("phase=bind;")));
        client.prepare(omit_sql).await.unwrap()
    } else {
        omit
    };
    client.execute(&omit, &[&6i32]).await.unwrap();

    let expressions = client.query(
        "SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid = 'd'::regclass
         ORDER BY adnum", &[],
    ).await.unwrap();
    assert_eq!(
        expressions.iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>(),
        ["random(5, 6)", expected[0], expected[3], expected[4], expected[2]],
    );

    let rows = client.query(
        "SELECT r BETWEEN 1 AND 2, r BETWEEN 5 AND 6, n BETWEEN 1.5 AND 2.25,
         uuid_extract_timestamp(u) > now() + interval '23 hours', v, ts IS NOT NULL
         FROM d ORDER BY id", &[],
    ).await.unwrap();
    assert_eq!(rows.len(), 6);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!((row.get::<_, bool>(0), row.get::<_, bool>(1)), (i < 4, i >= 4));
        assert!(row.get::<_, bool>(2));
        assert!(row.get::<_, bool>(3));
        assert_eq!(row.get::<_, i16>(4), 7);
        assert!(row.get::<_, bool>(5));
    }
}

fn check_duplicate(
    err: tokio_postgres::Error,
    constraint: &str,
    message: &str,
    detail: Option<&str>,
) {
    let err = err.as_db_error().unwrap();
    assert_eq!(err.code().code(), "23505");
    assert_eq!(err.message(), message);
    assert_eq!(err.schema(), Some("private"));
    assert_eq!(err.table(), Some("User"));
    assert_eq!(err.constraint(), Some(constraint));
    assert_eq!(err.detail(), detail);
}

async fn check_duplicate_visibility(owner: &tokio_postgres::Client, writer_config: Config) {
    // Legacy table ownership checks require a committed schema.
    owner.batch_execute("CREATE SCHEMA private").await.unwrap();
    owner.batch_execute(
        r#"CREATE TABLE private."User" (
            id uuid DEFAULT gen_random_uuid() CONSTRAINT "Private_pkey" PRIMARY KEY,
            email text, secret uuid, bucket int,
            CONSTRAINT "Private_secret_key" UNIQUE (secret));
        CREATE UNIQUE INDEX "Private_email_key" ON private."User" (email);"#,
    ).await.unwrap();
    for sql in [
        "CREATE ROLE am_writer LOGIN",
        "GRANT USAGE ON SCHEMA private TO am_writer",
        r#"GRANT INSERT ON private."User" TO am_writer"#,
    ] {
        owner.batch_execute(sql).await.unwrap();
    }
    let (writer, connection) = writer_config.connect(NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    let id = "00000000-0000-0000-0000-000000000001";
    let secret = "00000000-0000-0000-0000-000000000002";
    writer.batch_execute(
        &format!(r#"INSERT INTO private."User" VALUES ('{id}','a@x','{secret}',1), (DEFAULT,'b@x',NULL,1)"#),
    ).await.unwrap();
    let email = writer.prepare(r#"INSERT INTO private."User"(email) VALUES($1)"#).await.unwrap();
    let email_message = r#"duplicate key value violates unique constraint "Private_email_key""#;
    check_duplicate(writer.execute(&email, &[&"a@x"]).await.unwrap_err(), "Private_email_key", email_message, None);
    check_duplicate(
        writer.batch_execute(&format!(r#"INSERT INTO private."User"(id,email) VALUES('{id}','c@x')"#)).await.unwrap_err(),
        "Private_pkey", r#"duplicate key value violates unique constraint "Private_pkey""#, None,
    );
    check_duplicate(
        writer.batch_execute(&format!(r#"INSERT INTO private."User"(email,secret) VALUES('c@x','{secret}')"#)).await.unwrap_err(),
        "Private_secret_key", r#"duplicate key value violates unique constraint "Private_secret_key""#, None,
    );

    let sink = writer.copy_in(r#"COPY private."User" (email) FROM STDIN (FORMAT BINARY)"#).await.unwrap();
    let copy = tokio_postgres::binary_copy::BinaryCopyInWriter::new(sink, &[tokio_postgres::types::Type::TEXT]);
    let mut copy = std::pin::pin!(copy);
    copy.as_mut().write(&[&"a@x"]).await.unwrap();
    check_duplicate(copy.as_mut().finish().await.unwrap_err(), "Private_email_key", email_message, None);

    owner.batch_execute("SET ROLE am_writer").await.unwrap();
    check_duplicate(
        owner.batch_execute(r#"INSERT INTO private."User"(email) VALUES('a@x')"#).await.unwrap_err(),
        "Private_email_key", email_message, None,
    );
    owner.batch_execute("RESET ROLE").await.unwrap();
    check_duplicate(
        owner.batch_execute(r#"CREATE UNIQUE INDEX "Private_bucket_key" ON private."User"(bucket)"#).await.unwrap_err(),
        "Private_bucket_key", r#"could not create unique index "Private_bucket_key""#,
        Some("Key (bucket)=(1) is duplicated."),
    );

    owner.batch_execute(r#"GRANT SELECT ON private."User" TO am_writer"#).await.unwrap();
    check_duplicate(
        writer.execute(&email, &[&"a@x"]).await.unwrap_err(), "Private_email_key", email_message,
        Some("Key (email)=(a@x) already exists."),
    );
    drop(writer);
    connection.await.unwrap().unwrap();
}
