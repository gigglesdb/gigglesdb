#[tokio::test]
async fn partial_indexes_archdocs_migration_and_upsert() {
    use giggles_core::roles::NewRole;
    use giggles_server::auth::{PostgresqlConfig, TlsMode, serve_postgresql};

    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "partial-indexes-network-{}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed),
    ));
    std::fs::create_dir_all(&path).unwrap();
    let engine = if cfg!(feature = "engine-rocksdb") {
        giggles_core::Engine::V2
    } else {
        giggles_core::Engine::Legacy
    };
    let admin = Admin::create_engine(path.join("database"), Limits::default(), engine).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            superuser: true,
            login: true,
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
        certificates: Default::default(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 4,
    };
    let running = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(running.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);

    client.batch_execute(r#"
        CREATE TYPE st AS ENUM ('PENDING','AUTHORIZED','DRAFT','QUEUED','CREATED','OPEN','CLOSED');
        CREATE TABLE "ArchitectureDocs" (id int PRIMARY KEY, "ownerObjectId" int, "ownerSlotKey" text);
        CREATE TABLE "PlanGrant" (id int PRIMARY KEY, "organizationId" int, "revokedAt" timestamp);
        CREATE TABLE "LinearOutbox" (id int PRIMARY KEY, "installationId" int, "dedupeKey" text, status st);
        CREATE TABLE "DocReviewSignal" (id int PRIMARY KEY, "architectureDocId" int, "linearIssueRowId" int, "resolvedAt" timestamp);
        CREATE TABLE "Space" (id int PRIMARY KEY, "organizationId" int, "isDefault" boolean, "deletedAt" timestamp(3), label text);
        CREATE TABLE "CriteriaBaseline" (id int PRIMARY KEY, "designDocId" int, status st);
        CREATE TABLE "LinearIssueProposalItem" (id int PRIMARY KEY, "designDocId" int, "taskKey" text, status st);
        CREATE TABLE "DesignDocRelation" (id int PRIMARY KEY, "fromDocId" int, "toDocId" int, "retractedInVersion" int);
        CREATE TABLE "AdrAmendment" (id int PRIMARY KEY, "adrId" int, kind text, status st);
        CREATE TABLE "AdrRelation" (id int PRIMARY KEY, "fromAdrId" int, "toAdrId" int, "retractedAt" timestamp);
    "#).await.unwrap();

    let migrations = [
        r#"CREATE UNIQUE INDEX "ArchitectureDocs_owner_slot_unique" ON "ArchitectureDocs"("ownerObjectId", "ownerSlotKey") WHERE "ownerObjectId" IS NOT NULL AND "ownerSlotKey" IS NOT NULL"#,
        r#"CREATE UNIQUE INDEX "PlanGrant_one_unrevoked_per_org" ON "PlanGrant"("organizationId") WHERE "revokedAt" IS NULL"#,
        r#"CREATE UNIQUE INDEX "LinearOutbox_pending_dedupe_unique" ON "LinearOutbox" ("installationId", "dedupeKey") WHERE "status" = 'PENDING' AND "dedupeKey" IS NOT NULL"#,
        r#"CREATE UNIQUE INDEX "DocReviewSignal_open_unique" ON "DocReviewSignal" ("architectureDocId", "linearIssueRowId") WHERE "resolvedAt" IS NULL"#,
        r#"CREATE UNIQUE INDEX "Space_one_live_default_per_org" ON "Space" ("organizationId") WHERE "isDefault" AND "deletedAt" IS NULL"#,
        r#"CREATE UNIQUE INDEX "CriteriaBaseline_one_authorized" ON "CriteriaBaseline"("designDocId") WHERE "status" = 'AUTHORIZED'"#,
        r#"CREATE UNIQUE INDEX "CriteriaBaseline_one_draft" ON "CriteriaBaseline"("designDocId") WHERE "status" = 'DRAFT'"#,
        r#"CREATE UNIQUE INDEX "LinearIssueProposalItem_one_issue_per_task" ON "LinearIssueProposalItem" ("designDocId", "taskKey") WHERE "status" IN ('QUEUED', 'CREATED')"#,
        r#"CREATE UNIQUE INDEX "DesignDocRelation_active_pair_key" ON "DesignDocRelation"("fromDocId", "toDocId") WHERE "retractedInVersion" IS NULL"#,
        r#"CREATE UNIQUE INDEX "AdrAmendment_one_open_per_adr" ON "AdrAmendment"("adrId") WHERE "status" = 'OPEN'"#,
        r#"CREATE UNIQUE INDEX "AdrAmendment_one_open_per_adr_kind" ON "AdrAmendment"("adrId", "kind") WHERE "status" = 'OPEN'"#,
        r#"CREATE UNIQUE INDEX "AdrRelation_active_pair_key" ON "AdrRelation"("fromAdrId", "toAdrId") WHERE "retractedAt" IS NULL"#,
    ];
    for (index, sql) in migrations.into_iter().enumerate() {
        if index % 2 == 0 {
            client
                .batch_execute(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        } else {
            client
                .execute(sql, &[])
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        }
    }

    client
        .batch_execute(
            r#"
        INSERT INTO "Space" VALUES (1, 8, true, NULL, 'first'), (2, 8, false, NULL, 'other');
        INSERT INTO "Space" VALUES (3, 8, true, NULL, 'skip')
        ON CONFLICT ("organizationId") WHERE "isDefault" AND "deletedAt" IS NULL DO NOTHING;
    "#,
        )
        .await
        .unwrap();
    let upsert = r#"
        INSERT INTO "Space" (id, "organizationId", "isDefault", label) VALUES ($1, $2, true, $3)
        ON CONFLICT ("organizationId") WHERE "deletedAt" IS NULL AND "isDefault"
        DO UPDATE SET label=excluded.label RETURNING id, label
    "#;
    let row = client
        .query_one(upsert, &[&4i32, &8i32, &"updated"])
        .await
        .unwrap();
    assert_eq!(
        (row.get::<_, i32>(0), row.get::<_, String>(1)),
        (1, "updated".into())
    );

    let conflict = client
        .simple_query(r#"INSERT INTO "Space" VALUES (5, 8, true, NULL, 'duplicate')"#)
        .await
        .unwrap_err();
    let conflict = conflict.as_db_error().unwrap();
    assert_eq!(conflict.code().code(), "23505");
    assert_eq!(
        conflict.message(),
        r#"duplicate key value violates unique constraint "Space_one_live_default_per_org""#
    );
    assert_eq!(
        conflict.detail(),
        Some(r#"Key ("organizationId")=(8) already exists."#)
    );

    let row = client.query_one(r#"SELECT i.indpred IS NOT NULL, pg_get_expr(i.indpred, i.indrelid), pg_get_indexdef(i.indexrelid)
        FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname = 'Space_one_live_default_per_org'"#, &[]).await.unwrap();
    assert!(row.get::<_, bool>(0));
    assert_eq!(
        row.get::<_, String>(1),
        r#"("isDefault" AND ("deletedAt" IS NULL))"#
    );
    assert_eq!(
        row.get::<_, String>(2),
        r#"CREATE UNIQUE INDEX "Space_one_live_default_per_org" ON public."Space" USING btree ("organizationId") WHERE ("isDefault" AND ("deletedAt" IS NULL))"#
    );

    client.batch_execute(r#"
        CREATE TABLE docs (id int PRIMARY KEY, "organizationId" int, "createdAt" timestamp, "deletedAt" timestamp);
        CREATE INDEX docs_live_org ON docs("organizationId") WHERE "deletedAt" IS NULL;
        CREATE INDEX docs_live_created ON docs("createdAt") WHERE "deletedAt" IS NULL;
        INSERT INTO docs VALUES (1,8,'2024-02-01',NULL), (2,8,'2024-02-01','2024-02-02');
    "#).await.unwrap();
    let joins = client.query(
        r#"SELECT d.id FROM "Space" s JOIN docs d ON d."organizationId" = s."organizationId"
           WHERE s.id = $1 ORDER BY d.id"#,
        &[&1i32],
    ).await.unwrap();
    assert_eq!(joins.iter().map(|row| row.get::<_, i32>(0)).collect::<Vec<_>>(), [1, 2]);
    let range = client.query(
        r#"SELECT id FROM docs WHERE "createdAt" > '2024-01-01' ORDER BY id"#,
        &[],
    ).await.unwrap();
    assert_eq!(range.iter().map(|row| row.get::<_, i32>(0)).collect::<Vec<_>>(), [1, 2]);

    for (predicate, state) in [
        (r#""createdAt" > '2024-01-01'::timestamp with time zone"#, "42P17"),
        ("'abc'", "22P02"),
        ("id > $1", "42P02"),
    ] {
        let error = client.batch_execute(&format!("CREATE INDEX bad ON docs(id) WHERE {predicate}"))
            .await.unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), state);
    }

    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
