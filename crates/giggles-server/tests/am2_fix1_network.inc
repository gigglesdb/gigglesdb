use super::*;

#[tokio::test]
async fn prisma_enum_arrays_and_label_oids_through_a_migration() {
    let engine = if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    };
    let f = Fixture::with_engine(engine).await;
    let pg = f.client().await;
    // Legacy table ownership checks require the schema to be committed.
    pg.simple_query("CREATE SCHEMA design").await.unwrap();
    // Keep two enum-array name collisions on both engines. SQL view DDL needs V2.
    let second_collision = if engine == Engine::V2 {
        r#"CREATE VIEW "_DesignDocSize_1" AS SELECT id FROM "_DesignDocSize""#
    } else {
        r#"CREATE TABLE "_DesignDocSize_1"(id int PRIMARY KEY)"#
    };
    pg.simple_query(&format!(r#"
        SET search_path=design,public;
        CREATE TABLE "_DesignDocSize"(id int PRIMARY KEY);
        {second_collision};
        CREATE TYPE "DesignDocSize" AS ENUM ('SMALL', 'STANDARD', 'LARGE');
    "#)).await.unwrap();

    let labels_sql = r#"SELECT enumlabel, oid FROM pg_enum WHERE enumtypid='"DesignDocSize"'::regtype ORDER BY enumsortorder"#;
    let initial_labels = rows(&pg, labels_sql).await;
    pg.simple_query(r#"
        BEGIN;
        ALTER TYPE "DesignDocSize" ADD VALUE 'COMPACT' BEFORE 'STANDARD';
        ALTER TYPE "DesignDocSize" ADD VALUE 'REVIEW' AFTER 'STANDARD';
        ALTER TYPE "DesignDocSize" ADD VALUE 'HUGE';
        ALTER TYPE "DesignDocSize" RENAME VALUE 'COMPACT' TO 'MEDIUM';
        COMMIT;
    "#).await.unwrap();
    let labels = rows(&pg, labels_sql).await;
    assert_eq!(labels.iter().map(|row| row[0].as_deref().unwrap()).collect::<Vec<_>>(),
        ["SMALL", "MEDIUM", "STANDARD", "REVIEW", "LARGE", "HUGE"]);
    for (before, after) in initial_labels.iter().zip([0, 2, 4]) {
        assert_eq!(before, &labels[after]);
    }
    pg.simple_query(r#"ALTER TYPE "DesignDocSize" RENAME VALUE 'MEDIUM' TO 'COMPACT'"#).await.unwrap();
    let renamed = rows(&pg, labels_sql).await;
    assert_eq!(renamed[1][1], labels[1][1]);
    for relation in ["pg_type", "pg_class", "pg_constraint"] {
        expect_rows(&pg, &format!("SELECT count(*) FROM pg_enum e JOIN {relation} c ON c.oid=e.oid"),
            &[&[Some("0")]]).await;
    }
    expect_rows(&pg, r#"SELECT a.typname FROM pg_type e JOIN pg_type a ON a.oid=e.typarray WHERE e.typname='DesignDocSize'"#,
        &[&[Some("_DesignDocSize_2")]]).await;

    pg.simple_query(r#"
        CREATE TABLE "DesignDocVariationType" ("id" UUID NOT NULL, "presetKey" TEXT, "requiredFor" "DesignDocSize"[], "approvalFor" "DesignDocSize"[] DEFAULT ARRAY['LARGE']::"DesignDocSize"[], CONSTRAINT "DesignDocVariationType_pkey" PRIMARY KEY ("id"));
        ALTER TABLE "DesignDocVariationType" ADD COLUMN "approvalCounts" JSONB NOT NULL DEFAULT '{"SMALL":0,"STANDARD":0,"LARGE":0}', ADD COLUMN "selfApproveAgentWork" BOOLEAN NOT NULL DEFAULT true;
        ALTER TABLE "DesignDocVariationType" ALTER COLUMN "approvalFor" SET DEFAULT ARRAY[]::"DesignDocSize"[];
    "#).await.unwrap();

    let inserted = pg.execute(r#"
        INSERT INTO "DesignDocVariationType" ("id", "presetKey", "requiredFor") VALUES
        ('00000000-0000-0000-0000-000000000001', $1, ARRAY['SMALL',NULL]::"DesignDocSize"[]),
        ('00000000-0000-0000-0000-000000000002', 'other', ARRAY['LARGE']::"DesignDocSize"[]),
        ('00000000-0000-0000-0000-000000000003', 'missing', NULL)
    "#, &[&"evidence"]).await.unwrap();
    assert_eq!(inserted, 3);
    pg.simple_query(r#"
        UPDATE "DesignDocVariationType" SET "requiredFor" = ARRAY[]::"DesignDocSize"[] WHERE "presetKey" = 'evidence';
        INSERT INTO "DesignDocVariationType" ("id", "presetKey", "requiredFor")
            SELECT '00000000-0000-0000-0000-000000000004'::uuid, 'later'::text, ARRAY['STANDARD']::"DesignDocSize"[];
        RESET search_path;
    "#).await.unwrap();

    let selected = pg.query(r#"SELECT "presetKey", "requiredFor"::text AS required_text FROM design."DesignDocVariationType" ORDER BY "requiredFor" NULLS LAST"#, &[])
        .await.unwrap();
    assert_eq!(selected.iter().map(|row| (row.get::<_, &str>(0), row.get::<_, Option<&str>>(1))).collect::<Vec<_>>(),
        [("evidence", Some("{}")), ("later", Some("{STANDARD}")), ("other", Some("{LARGE}")), ("missing", None)]);
    expect_rows(&pg, r#"SELECT pg_typeof("requiredFor"), 'STANDARD'=ANY("requiredFor")
        FROM design."DesignDocVariationType" WHERE "presetKey"='later'"#,
        &[&[Some(r#"design."DesignDocSize"[]"#), Some("t")]]).await;
    let err = pg.simple_query(r#"SELECT 'BOGUS' = ANY(ARRAY['SMALL']::design."DesignDocSize"[])"#).await.unwrap_err();
    let err = err.as_db_error().unwrap();
    assert_eq!(err.code().code(), "22P02");
    assert_eq!(err.message(), r#"invalid input value for enum design."DesignDocSize": "BOGUS""#);

    expect_rows(&pg, r#"SELECT is_nullable,data_type,udt_schema,udt_name FROM information_schema.columns
        WHERE table_schema='design' AND table_name='DesignDocVariationType' AND column_name='requiredFor'"#,
        &[&[Some("YES"), Some("ARRAY"), Some("design"), Some("_DesignDocSize_2")]]).await;
    expect_rows(&pg, r#"SELECT pg_get_expr(d.adbin,d.adrelid) FROM pg_attrdef d
        JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum
        WHERE d.adrelid='design."DesignDocVariationType"'::regclass AND a.attname='approvalFor'"#,
        &[&[Some(r#"ARRAY[]::design."DesignDocSize"[]"#)]]).await;
    expect_rows(&pg, r#"SELECT conname,contype FROM pg_constraint
        WHERE conrelid='design."DesignDocVariationType"'::regclass AND contype='p'"#,
        &[&[Some("DesignDocVariationType_pkey"), Some("p")]]).await;
    expect_rows(&pg, r#"SELECT i.relname,x.indisprimary,x.indisunique FROM pg_index x
        JOIN pg_class i ON i.oid=x.indexrelid WHERE x.indrelid='design."DesignDocVariationType"'::regclass"#,
        &[&[Some("DesignDocVariationType_pkey"), Some("t"), Some("t")]]).await;
    drop(pg);
    f.stop().await;
}
