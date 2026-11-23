use bytes::{BufMut, BytesMut};
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, error::Error, path::Path, sync::Arc, time::UNIX_EPOCH};
use tokio_postgres::{
    Config, NoTls,
    types::{IsNull, ToSql, Type, to_sql_checked},
};

#[derive(Debug)]
struct BinaryUuid(u8);

impl ToSql for BinaryUuid {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        out.put_u128(u128::from(self.0));
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::UUID
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct BinaryTimestamp(i64);

impl ToSql for BinaryTimestamp {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        out.put_i64(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TIMESTAMP
    }

    to_sql_checked!();
}

#[tokio::test]
async fn prisma_temporal_index_migration_and_binary_binds() {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "temporal-index-network-{}-{}",
        std::process::id(),
        UNIX_EPOCH.elapsed().unwrap().as_nanos(),
    ));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
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
            login: true,
            superuser: true,
            ..NewRole::default()
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
        connections: 4,
    };
    let running = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let mut config = Config::new();
    config
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("owner")
        .dbname("postgres");
    let (owner, connection) = config.connect(NoTls).await.unwrap();
    let owner_connection = tokio::spawn(connection);
    owner.batch_execute(r#"
        CREATE TABLE "Organization" (id uuid PRIMARY KEY);
        CREATE TABLE "DesignDocSettings" (
            "organizationId" uuid PRIMARY KEY REFERENCES "Organization"(id) ON DELETE CASCADE,
            "approvals" boolean NOT NULL DEFAULT false,
            pg_x integer NOT NULL DEFAULT 0);
        CREATE TABLE "Obj" (id uuid PRIMARY KEY,
            "organizationId" uuid NOT NULL REFERENCES "Organization"(id),
            "createdAt" timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "deletedAt" timestamp(3));
        CREATE INDEX "Obj_c_idx" ON "Obj" ("createdAt");
        CREATE INDEX "Obj_org_del_idx" ON "Obj" ("organizationId", "deletedAt");
        CREATE UNIQUE INDEX "Obj_org_created_key" ON "Obj" ("organizationId", "createdAt");
        INSERT INTO "Organization" VALUES ('00000000-0000-0000-0000-000000000001');
        INSERT INTO "DesignDocSettings" ("organizationId") VALUES ('00000000-0000-0000-0000-000000000001');
        CREATE TABLE uq_ts (id int PRIMARY KEY, t timestamptz UNIQUE);
    "#).await.unwrap();
    owner
        .batch_execute("CREATE ROLE temporal_writer LOGIN")
        .await
        .unwrap();
    owner
        .batch_execute(r#"GRANT INSERT ON "Obj" TO temporal_writer"#)
        .await
        .unwrap();
    owner
        .batch_execute(r#"GRANT INSERT ON "DesignDocSettings" TO temporal_writer"#)
        .await
        .unwrap();

    let info = owner
        .query_one(
            "SELECT indexdef FROM pg_indexes WHERE indexname='Obj_c_idx'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        info.get::<_, String>(0),
        r#"CREATE INDEX "Obj_c_idx" ON public."Obj" USING btree ("createdAt")"#
    );
    let info = owner.query_one(r#"SELECT i.indisunique,i.indnatts,i.indkey::text,
        i.indpred IS NULL,i.indoption::text,pg_get_indexdef(i.indexrelid)
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname='Obj_org_created_key'"#,
        &[],
    ).await.unwrap();
    assert!(info.get::<_, bool>(0));
    assert_eq!(info.get::<_, i16>(1), 2);
    assert_eq!(info.get::<_, String>(2), "2 3");
    assert!(info.get::<_, bool>(3));
    assert_eq!(info.get::<_, String>(4), "0 0");
    assert_eq!(
        info.get::<_, String>(5),
        r#"CREATE UNIQUE INDEX "Obj_org_created_key" ON public."Obj" USING btree ("organizationId", "createdAt")"#
    );
    let info = owner
        .query_one(
            r#"SELECT pg_get_constraintdef(oid) FROM pg_constraint
        WHERE conname='DesignDocSettings_organizationId_fkey'"#,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        info.get::<_, String>(0),
        r#"FOREIGN KEY ("organizationId") REFERENCES "Organization"(id) ON DELETE CASCADE"#
    );
    let info = owner
        .query_one(
            r#"SELECT pg_get_constraintdef(oid) FROM pg_constraint
        WHERE conname='uq_ts_t_key'"#,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(info.get::<_, String>(0), "UNIQUE (t)");
    let missing = owner
        .execute(
            r#"INSERT INTO "DesignDocSettings" ("organizationId") VALUES ($1)"#,
            &[&BinaryUuid(9)],
        )
        .await
        .unwrap_err();
    let error = missing.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23503");
    assert_eq!(
        error.message(),
        r#"insert or update on table "DesignDocSettings" violates foreign key constraint "DesignDocSettings_organizationId_fkey""#
    );
    assert_eq!(
        error.detail(),
        Some(
            r#"Key (organizationId)=(00000000-0000-0000-0000-000000000009) is not present in table "Organization"."#
        )
    );
    assert_eq!(error.table(), Some("DesignDocSettings"));
    assert_eq!(
        error.constraint(),
        Some("DesignDocSettings_organizationId_fkey")
    );

    let insert = r#"INSERT INTO "Obj" (id,"organizationId","createdAt") VALUES ($1,$2,$3)"#;
    let statement = owner.prepare(insert).await.unwrap();
    assert_eq!(
        statement.params(),
        [Type::UUID, Type::UUID, Type::TIMESTAMP]
    );
    let first = BinaryTimestamp(757_427_400_123_000);
    let second = BinaryTimestamp(757_427_400_123_600);
    for (id, time) in [(2, &first), (3, &second)] {
        assert_eq!(
            owner
                .execute(&statement, &[&BinaryUuid(id), &BinaryUuid(1), time])
                .await
                .unwrap(),
            1
        );
    }
    owner
        .execute(
            r#"INSERT INTO "Obj" (id, "organizationId") VALUES ($1,$2)"#,
            &[&BinaryUuid(4), &BinaryUuid(1)],
        )
        .await
        .unwrap();
    let settings = owner
        .query_one(
            r#"UPDATE "DesignDocSettings" SET approvals=$2, pg_x=$3
                WHERE "organizationId"=$1 RETURNING approvals, pg_x"#,
            &[&BinaryUuid(1), &true, &7_i32],
        )
        .await
        .unwrap();
    assert!(settings.get::<_, bool>(0));
    assert_eq!(settings.get::<_, i32>(1), 7);
    let read = owner
        .prepare(
            r#"SELECT "createdAt"::text FROM "Obj"
        WHERE "createdAt">=$1 AND "createdAt"<$2 ORDER BY "createdAt" DESC"#,
        )
        .await
        .unwrap();
    let rows = owner
        .query(&read, &[&first, &BinaryTimestamp(757_427_401_000_000)])
        .await
        .unwrap();
    let times = rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    assert_eq!(
        times,
        ["2024-01-01 12:30:00.124", "2024-01-01 12:30:00.123"]
    );

    let duplicate = owner
        .execute(&statement, &[&BinaryUuid(5), &BinaryUuid(1), &first])
        .await
        .unwrap_err();
    let error = duplicate.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23505");
    assert_eq!(
        error.message(),
        r#"duplicate key value violates unique constraint "Obj_org_created_key""#
    );
    assert_eq!(
        error.detail(),
        Some(
            r#"Key ("organizationId", "createdAt")=(00000000-0000-0000-0000-000000000001, 2024-01-01 12:30:00.123) already exists."#
        )
    );
    assert_eq!(error.schema(), Some("public"));
    assert_eq!(error.table(), Some("Obj"));
    assert_eq!(error.constraint(), Some("Obj_org_created_key"));

    let (writer, connection) = config.user("temporal_writer").connect(NoTls).await.unwrap();
    let writer_connection = tokio::spawn(connection);
    let duplicate = writer
        .execute(insert, &[&BinaryUuid(5), &BinaryUuid(1), &first])
        .await
        .unwrap_err();
    let error = duplicate.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23505");
    assert_eq!(error.detail(), None);
    let missing = writer
        .execute(
            r#"INSERT INTO "DesignDocSettings" ("organizationId") VALUES ($1)"#,
            &[&BinaryUuid(9)],
        )
        .await
        .unwrap_err();
    let error = missing.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23503");
    assert_eq!(
        error.detail(),
        Some(r#"Key is not present in table "Organization"."#)
    );

    owner
        .batch_execute(
            "SET TIME ZONE 'Europe/Paris';
        CREATE TABLE zoned (id int PRIMARY KEY, at timestamptz UNIQUE);
        INSERT INTO zoned VALUES (1,'2024-01-01 00:00+00');",
        )
        .await
        .unwrap();
    let duplicate = owner
        .simple_query("INSERT INTO zoned VALUES (2,'2024-01-01 00:00+00')")
        .await
        .unwrap_err();
    assert_eq!(
        duplicate.as_db_error().unwrap().detail(),
        Some("Key (at)=(2024-01-01 01:00:00+01) already exists.")
    );
    owner
        .batch_execute(
            "SET DATESTYLE TO 'SQL, DMY';
        CREATE TABLE historical (id int PRIMARY KEY, d date UNIQUE);
        INSERT INTO historical VALUES (1,'2024-01-02');",
        )
        .await
        .unwrap();
    let duplicate = owner
        .simple_query("INSERT INTO historical VALUES (2,'2024-01-02')")
        .await
        .unwrap_err();
    assert_eq!(
        duplicate.as_db_error().unwrap().detail(),
        Some("Key (d)=(02/01/2024) already exists.")
    );
    drop(writer);
    drop(owner);
    writer_connection.await.unwrap().unwrap();
    owner_connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
