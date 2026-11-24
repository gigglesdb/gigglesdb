use giggles_core::{Admin, Limits};
use giggles_server::{Service, clock::Clock};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[tokio::test]
async fn enum_column_oids_agree_across_protocols() {
    use super::wire_sql_types::{
        api_support::{rpc, sql},
        fixture, http_result, setup,
    };
    use giggles_server::{
        api::proto,
        auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    };

    let (f, wire, mut native, _) = fixture().await;
    setup(&f, "CREATE TYPE migration_stage AS ENUM ('DRAFT', 'READY')");
    setup(
        &f,
        "CREATE TABLE migration_oids (stage migration_stage, stages migration_stage[])",
    );
    setup(
        &f,
        "INSERT INTO migration_oids VALUES ('READY', '{DRAFT,READY}')",
    );

    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: Default::default(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 8,
    };
    let postgres = serve_postgresql(
        config.listen,
        f.service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let (pg, connection) = tokio_postgres::Config::new()
        .hostaddr(postgres.address.ip())
        .port(postgres.address.port())
        .user("oid_owner")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);

    let catalog = pg
        .query_one(
            "SELECT oid, typarray FROM pg_type WHERE typname = 'migration_stage'",
            &[],
        )
        .await
        .unwrap();
    let expected: [u32; 2] = [catalog.get(0), catalog.get(1)];
    assert!(expected.iter().all(|&oid| oid >= 65536));
    assert_ne!(expected[0], expected[1]);
    let query = "SELECT stage, stages FROM migration_oids";
    let description = pg.prepare(query).await.unwrap();
    let pg_oids: Vec<_> = description
        .columns()
        .iter()
        .map(|column| column.type_().oid())
        .collect();
    assert_eq!(pg_oids, expected);

    let http = http_result(&f, query).await;
    assert_eq!(http.column_types, pg_oids);
    assert_eq!(http.rows.len(), 1);

    let mut grpc = f.grpc_client(&f.app).await;
    let session = grpc
        .open_session(proto::OpenSessionRequest {
            role: Some("oid_owner".into()),
            database: Some("postgres".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let reply = grpc
        .execute(rpc(&session.session_id, sql(query)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.code, "ok", "{reply:?}");
    assert_eq!(reply.sql.unwrap().column_types, pg_oids);
    grpc.close_session(rpc(&session.session_id, proto::CloseSessionRequest {}))
        .await
        .unwrap();

    // The first reply defines the shape, and the second reuses it.
    for _ in 0..2 {
        let reply = native.execute(sql(query)).await.unwrap();
        assert_eq!(reply.code, "ok", "{reply:?}");
        let result = reply.sql.unwrap();
        assert_eq!(result.columns, http.columns);
        assert_eq!(result.column_types, pg_oids);
    }

    native.goodbye().await.unwrap();
    wire.shutdown().await.unwrap();
    drop(pg);
    connection.await.unwrap().unwrap();
    postgres.shutdown().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn am2_enum_and_primary_key_archdocs_migration() {
    use giggles_core::{Engine, roles::NewRole};
    use giggles_server::auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql};
    use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

    async fn rows(client: &Client, sql: &str) -> Vec<Vec<String>> {
        client
            .simple_query(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
            .into_iter()
            .filter_map(|message| {
                let SimpleQueryMessage::Row(row) = message else {
                    return None;
                };
                Some(
                    (0..row.len())
                        .map(|column| row.get(column).unwrap().to_owned())
                        .collect(),
                )
            })
            .collect()
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("am2-wire-{}-{timestamp}", std::process::id()));
    let engine = if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    };
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            superuser: true,
            login: true,
            createrole: true,
            password: Some(Verifier::generate("test-password")),
            ..NewRole::default()
        })
        .unwrap();
    let service = Service::new(admin, Vec::new(), Clock::new(0, 3600).unwrap()).unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: Default::default(),
        databases: vec!["postgres".into()],
        loopback_trust: false,
        connections: 8,
    };
    let listener = Arc::new(config.build().unwrap());
    let running = serve_postgresql(config.listen, service.clone(), listener)
        .await
        .unwrap();
    let (pg, connection) = tokio_postgres::Config::new()
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("owner")
        .password("test-password")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);

    pg.simple_query(r#"
        CREATE TYPE designdocstatus_placeholder AS ENUM ('DRAFT', 'IN_REVIEW', 'APPROVED', 'SHIPPED');
        CREATE TABLE "RefreshToken" ("userId" INTEGER NOT NULL, "token" TEXT NOT NULL, CONSTRAINT "RefreshToken_pkey" PRIMARY KEY ("userId"));
        INSERT INTO "RefreshToken" VALUES (1,'t1'),(2,'t2');
    "#).await.unwrap();

    pg.simple_query(r#"
        BEGIN;
        CREATE TYPE "DesignDocStatus" AS ENUM ('DRAFT', 'IN_REVIEW', 'APPROVED', 'SHIPPED');
        CREATE TABLE "Doc" ("id" INTEGER NOT NULL, "status" designdocstatus_placeholder, CONSTRAINT "Doc_pkey" PRIMARY KEY ("id"));
        ALTER TYPE "DesignDocStatus" ADD VALUE 'READY' AFTER 'IN_REVIEW';
        ALTER TYPE "DesignDocStatus" ADD VALUE IF NOT EXISTS 'VERIFYING';
        ALTER TYPE "DesignDocStatus" ADD VALUE 'ABANDONED';
        ALTER TABLE "RefreshToken" DROP CONSTRAINT "RefreshToken_pkey";
        ALTER TABLE "RefreshToken" ADD COLUMN "id" INTEGER;
        UPDATE "RefreshToken" SET "id" = "userId" + 10;
        ALTER TABLE "RefreshToken" ALTER COLUMN "id" SET NOT NULL;
        ALTER TABLE "RefreshToken" ADD CONSTRAINT "RefreshToken_pkey" PRIMARY KEY ("id");
        COMMIT;
    "#).await.unwrap();
    assert_eq!(rows(&pg, r#"SELECT enumlabel FROM pg_enum WHERE enumtypid='"DesignDocStatus"'::regtype ORDER BY enumsortorder"#).await,
        [["DRAFT"], ["IN_REVIEW"], ["READY"], ["APPROVED"], ["SHIPPED"], ["VERIFYING"], ["ABANDONED"]]);

    let error = pg.simple_query(r#"BEGIN; ALTER TYPE "DesignDocStatus" ADD VALUE 'BLOCKED'; SELECT 'BLOCKED'::"DesignDocStatus"; COMMIT"#)
        .await.unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "55P04");
    assert_eq!(
        error.message(),
        r#"unsafe use of new value "BLOCKED" of enum type "DesignDocStatus""#
    );
    assert_eq!(
        error.hint(),
        Some("New enum values must be committed before they can be used.")
    );
    pg.simple_query("ROLLBACK").await.unwrap();
    pg.simple_query(
        r#"
        BEGIN;
        ALTER TYPE "DesignDocStatus" ADD VALUE IF NOT EXISTS 'VERIFYING';
        ALTER TYPE "DesignDocStatus" ADD VALUE 'BLOCKED';
        ALTER TYPE "DesignDocStatus" RENAME VALUE 'BLOCKED' TO 'FINAL';
        COMMIT;
        ALTER TYPE designdocstatus_placeholder ADD VALUE 'READY' AFTER 'IN_REVIEW';
        ALTER TABLE "Doc" ALTER COLUMN "status" SET DEFAULT 'DRAFT';
    "#,
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&pg, r#"SELECT 'FINAL'::"DesignDocStatus""#).await,
        [["FINAL"]]
    );

    pg.execute(
        r#"INSERT INTO "Doc" (id,status) VALUES ($1,'READY')"#,
        &[&1i32],
    )
    .await
    .unwrap();
    pg.execute(r#"INSERT INTO "Doc" (id) VALUES ($1)"#, &[&2i32])
        .await
        .unwrap();
    let doc = pg
        .query(r#"SELECT id,status::text FROM "Doc" ORDER BY status"#, &[])
        .await
        .unwrap();
    assert_eq!(
        doc.iter()
            .map(|row| (row.get::<_, i32>(0), row.get::<_, &str>(1)))
            .collect::<Vec<_>>(),
        [(2, "DRAFT"), (1, "READY")]
    );
    assert_eq!(
        rows(
            &pg,
            r#"SELECT pg_get_expr(adbin,adrelid) FROM pg_attrdef WHERE adrelid='"Doc"'::regclass"#
        )
        .await,
        [["'DRAFT'::designdocstatus_placeholder"]]
    );
    assert_eq!(rows(&pg, "SELECT column_name,is_nullable,data_type,udt_name FROM information_schema.columns WHERE table_name='Doc' ORDER BY ordinal_position").await,
        [["id","NO","integer","int4"], ["status","YES","USER-DEFINED","designdocstatus_placeholder"]]);

    pg.simple_query(
        r#"
        CREATE INDEX doc_status_idx ON "Doc"(status);
        ALTER TYPE designdocstatus_placeholder RENAME VALUE 'DRAFT' TO 'DRAFTED';
    "#,
    )
    .await
    .unwrap();
    assert_eq!(
        rows(
            &pg,
            r#"SELECT id,status::text FROM "Doc" WHERE status='DRAFTED'"#
        )
        .await,
        [["2", "DRAFTED"]]
    );
    assert_eq!(
        rows(
            &pg,
            r#"SELECT pg_get_expr(adbin,adrelid) FROM pg_attrdef WHERE adrelid='"Doc"'::regclass"#
        )
        .await,
        [["'DRAFTED'::designdocstatus_placeholder"]]
    );

    let error = pg
        .execute(
            r#"INSERT INTO "RefreshToken" VALUES ($1,$2,$3)"#,
            &[&1i32, &"t3", &11i32],
        )
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23505");
    assert_eq!(
        error.message(),
        r#"duplicate key value violates unique constraint "RefreshToken_pkey""#
    );
    assert_eq!(error.detail(), Some("Key (id)=(11) already exists."));
    assert_eq!(
        pg.execute(
            r#"INSERT INTO "RefreshToken" VALUES ($1,$2,$3)"#,
            &[&1i32, &"t3", &13i32]
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        rows(
            &pg,
            r#"SELECT "userId",token,id FROM "RefreshToken" ORDER BY id"#
        )
        .await,
        [["1", "t1", "11"], ["2", "t2", "12"], ["1", "t3", "13"]]
    );
    assert_eq!(rows(&pg, r#"SELECT conname,contype,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='"RefreshToken"'::regclass ORDER BY 1"#).await,
        [
            ["RefreshToken_id_not_null","n","NOT NULL id"],
            ["RefreshToken_pkey","p","PRIMARY KEY (id)"],
            ["RefreshToken_token_not_null","n","NOT NULL token"],
            ["RefreshToken_userId_not_null","n",r#"NOT NULL "userId""#],
        ]);
    assert_eq!(rows(&pg, r#"SELECT i.relname,x.indisprimary,x.indisunique,pg_get_indexdef(x.indexrelid) FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid WHERE x.indrelid='"RefreshToken"'::regclass"#).await,
        [["RefreshToken_pkey","t","t",r#"CREATE UNIQUE INDEX "RefreshToken_pkey" ON public."RefreshToken" USING btree (id)"#]]);
    pg.simple_query(r#"ALTER TABLE "RefreshToken" DROP CONSTRAINT "RefreshToken_pkey"; INSERT INTO "RefreshToken" VALUES (1,'t4',11)"#).await.unwrap();
    assert!(rows(&pg, r#"SELECT conname FROM pg_constraint WHERE conrelid='"RefreshToken"'::regclass AND contype='p'"#).await.is_empty());
    assert!(
        rows(
            &pg,
            r#"SELECT indexrelid FROM pg_index WHERE indrelid='"RefreshToken"'::regclass"#
        )
        .await
        .is_empty()
    );
    let error = pg
        .simple_query(
            r#"ALTER TABLE "RefreshToken" ADD CONSTRAINT "RefreshToken_pkey" PRIMARY KEY ("id")"#,
        )
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23505");
    assert_eq!(
        error.message(),
        r#"could not create unique index "RefreshToken_pkey""#
    );
    assert_eq!(error.detail(), Some("Key (id)=(11) is duplicated."));
    pg.simple_query(r#"ALTER TABLE "RefreshToken" ADD CONSTRAINT "RefreshToken_pkey" PRIMARY KEY ("userId", "token")"#).await.unwrap();

    drop(pg);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
