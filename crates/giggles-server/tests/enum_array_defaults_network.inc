use super::*;

async fn rows(pg: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    pg.simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn expect_rows(pg: &tokio_postgres::Client, sql: &str, expected: &[&[Option<&str>]]) {
    let actual = rows(pg, sql).await;
    let expected = expected
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| value.map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{sql}");
}

#[tokio::test]
async fn prisma_enum_array_and_jsonb_defaults() {
    let engine = if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    };
    let f = Fixture::with_engine(engine).await;
    let pg = f.client().await;

    let migration = r#"
CREATE TYPE "SubscriptionStatus" AS ENUM ('TRIALING', 'ACTIVE', 'PAST_DUE', 'CANCELED');
CREATE TYPE "ObjectSource" AS ENUM ('MANUAL', 'IMPORT');
CREATE TYPE "DesignDocSize" AS ENUM ('SMALL', 'STANDARD', 'LARGE');
CREATE TABLE "Subscription" ("id" UUID NOT NULL, "organizationId" UUID NOT NULL, "status" "SubscriptionStatus" NOT NULL DEFAULT 'TRIALING', "seatCount" INTEGER NOT NULL DEFAULT 1, "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP, CONSTRAINT "Subscription_pkey" PRIMARY KEY ("id"));
CREATE TABLE "Object" ("id" UUID NOT NULL, "metadata" JSONB, "type" TEXT, "name" TEXT NOT NULL, CONSTRAINT "Object_pkey" PRIMARY KEY ("id"));
INSERT INTO "Object" VALUES ('00000000-0000-0000-0000-000000000001', '{}', 'legacy', 'existing');
ALTER TABLE "Object" DROP COLUMN "metadata",
DROP COLUMN "type",
ADD COLUMN     "kindId" UUID,
ADD COLUMN     "source" "ObjectSource" NOT NULL DEFAULT 'MANUAL';
CREATE TABLE "PersonalAccessToken" ("id" UUID NOT NULL, "scopes" TEXT[] DEFAULT ARRAY[]::TEXT[], CONSTRAINT "PersonalAccessToken_pkey" PRIMARY KEY ("id"));
CREATE TABLE "OAuthClient" ("id" UUID NOT NULL, "redirectUris" TEXT[], "grantTypes" TEXT[] DEFAULT ARRAY['authorization_code', 'refresh_token']::TEXT[], CONSTRAINT "OAuthClient_pkey" PRIMARY KEY ("id"));
INSERT INTO "OAuthClient" ("id", "redirectUris") VALUES ('00000000-0000-0000-0000-000000000001', '{https://example.org}');
CREATE TABLE "DesignDocVariationType" ("id" UUID NOT NULL, "presetKey" TEXT, "requiredFor" "DesignDocSize"[], "approvalFor" "DesignDocSize"[] DEFAULT ARRAY['LARGE']::"DesignDocSize"[], CONSTRAINT "DesignDocVariationType_pkey" PRIMARY KEY ("id"));
INSERT INTO "DesignDocVariationType" ("id", "presetKey", "requiredFor") VALUES ('00000000-0000-0000-0000-000000000001', 'evidence', '{STANDARD,SMALL}');
ALTER TABLE "DesignDocVariationType" ADD COLUMN "approvalCounts" JSONB NOT NULL DEFAULT '{"SMALL":0,"STANDARD":0,"LARGE":0}', ADD COLUMN "selfApproveAgentWork" BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE "DesignDocVariationType" ALTER COLUMN "approvalFor" SET DEFAULT ARRAY[]::"DesignDocSize"[];
UPDATE "DesignDocVariationType" SET "requiredFor" = ARRAY[]::"DesignDocSize"[] WHERE "presetKey" = 'evidence';
ALTER TABLE "OAuthClient" ADD COLUMN "excludedAccountIds" TEXT[] DEFAULT ARRAY[]::TEXT[], ADD COLUMN "orgExternalId" TEXT;
ALTER TABLE "OAuthClient" ADD COLUMN "linkDismissals" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[];
"#;
    let completions = pg
        .simple_query(migration)
        .await
        .unwrap()
        .into_iter()
        .filter(|message| matches!(message, SimpleQueryMessage::CommandComplete(_)))
        .count();
    assert_eq!(completions, 17);

    assert_eq!(pg.execute(
        r#"INSERT INTO "Subscription" ("id", "organizationId") VALUES ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002')"#,
        &[],
    ).await.unwrap(), 1);
    pg.batch_execute(r#"
        INSERT INTO "PersonalAccessToken" ("id") VALUES ('00000000-0000-0000-0000-000000000001');
        INSERT INTO "DesignDocVariationType" ("id", "presetKey", "requiredFor")
            VALUES ('00000000-0000-0000-0000-000000000002', 'later', ARRAY['LARGE','SMALL']::"DesignDocSize"[]);
    "#).await.unwrap();
    let subscription = pg
        .query_one(
            r#"SELECT status::text, "seatCount", "createdAt" IS NOT NULL FROM "Subscription""#,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(subscription.get::<_, &str>(0), "TRIALING");
    assert_eq!(subscription.get::<_, i32>(1), 1);
    assert!(subscription.get::<_, bool>(2));

    expect_rows(
        &pg,
        r#"SELECT name, "kindId", source FROM "Object""#,
        &[&[Some("existing"), None, Some("MANUAL")]],
    )
    .await;
    expect_rows(
        &pg,
        r#"SELECT scopes FROM "PersonalAccessToken""#,
        &[&[Some("{}")]],
    )
    .await;
    expect_rows(&pg, r#"SELECT "redirectUris", "grantTypes", "excludedAccountIds", "linkDismissals", "orgExternalId" FROM "OAuthClient""#,
        &[&[Some("{https://example.org}"), Some("{authorization_code,refresh_token}"), Some("{}"), Some("{}"), None]]).await;
    expect_rows(&pg, r#"SELECT "presetKey", "requiredFor", "approvalFor", "approvalCounts", "selfApproveAgentWork" FROM "DesignDocVariationType" ORDER BY "presetKey""#,
        &[
            &[Some("evidence"), Some("{}"), Some("{LARGE}"), Some(r#"{"LARGE": 0, "SMALL": 0, "STANDARD": 0}"#), Some("t")],
            &[Some("later"), Some("{LARGE,SMALL}"), Some("{}"), Some(r#"{"LARGE": 0, "SMALL": 0, "STANDARD": 0}"#), Some("t")],
        ]).await;

    for (sql, code, message) in [
        (
            r#"ALTER TABLE "Object" ADD COLUMN staged TEXT, ADD COLUMN bad "ObjectSource" DEFAULT 'NOPE'"#,
            "22P02",
            r#"invalid input value for enum "ObjectSource": "NOPE""#,
        ),
        (
            r#"UPDATE "DesignDocVariationType" SET "requiredFor" = ARRAY['NOPE']::"DesignDocSize"[] WHERE "presetKey" = 'later'"#,
            "22P02",
            r#"invalid input value for enum "DesignDocSize": "NOPE""#,
        ),
        (
            r#"ALTER TABLE "Object" ADD COLUMN staged TEXT, ADD COLUMN nn INTEGER NOT NULL"#,
            "23502",
            r#"column "nn" of relation "Object" contains null values"#,
        ),
    ] {
        let error = pg.simple_query(sql).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), code, "{sql}");
        assert_eq!(error.message(), message, "{sql}");
    }

    expect_rows(&pg, r#"SELECT column_name, is_nullable, data_type, udt_name FROM information_schema.columns WHERE table_name='Object' ORDER BY column_name"#,
        &[
            &[Some("id"), Some("NO"), Some("uuid"), Some("uuid")],
            &[Some("kindId"), Some("YES"), Some("uuid"), Some("uuid")],
            &[Some("name"), Some("NO"), Some("text"), Some("text")],
            &[Some("source"), Some("NO"), Some("USER-DEFINED"), Some("ObjectSource")],
        ]).await;
    expect_rows(&pg, r#"SELECT column_name, is_nullable, data_type, udt_name FROM information_schema.columns WHERE table_name='DesignDocVariationType' ORDER BY column_name"#,
        &[
            &[Some("approvalCounts"), Some("NO"), Some("jsonb"), Some("jsonb")],
            &[Some("approvalFor"), Some("YES"), Some("ARRAY"), Some("_DesignDocSize")],
            &[Some("id"), Some("NO"), Some("uuid"), Some("uuid")],
            &[Some("presetKey"), Some("YES"), Some("text"), Some("text")],
            &[Some("requiredFor"), Some("YES"), Some("ARRAY"), Some("_DesignDocSize")],
            &[Some("selfApproveAgentWork"), Some("NO"), Some("boolean"), Some("bool")],
        ]).await;
    expect_rows(&pg, r#"SELECT a.attname, pg_get_expr(d.adbin,d.adrelid) FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum WHERE d.adrelid='"DesignDocVariationType"'::regclass ORDER BY 1"#,
        &[
            &[Some("approvalCounts"), Some(r#"'{"LARGE": 0, "SMALL": 0, "STANDARD": 0}'::jsonb"#)],
            &[Some("approvalFor"), Some(r#"ARRAY[]::"DesignDocSize"[]"#)],
            &[Some("selfApproveAgentWork"), Some("true")],
        ]).await;
    expect_rows(&pg, r#"SELECT a.attname, pg_get_expr(d.adbin,d.adrelid) FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum WHERE d.adrelid='"OAuthClient"'::regclass ORDER BY 1"#,
        &[
            &[Some("excludedAccountIds"), Some("ARRAY[]::text[]")],
            &[Some("grantTypes"), Some("ARRAY['authorization_code'::text, 'refresh_token'::text]")],
            &[Some("linkDismissals"), Some("ARRAY[]::text[]")],
        ]).await;
    expect_rows(&pg, r#"SELECT a.attname, pg_get_expr(d.adbin,d.adrelid) FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum WHERE d.adrelid='"Object"'::regclass ORDER BY 1"#,
        &[&[Some("source"), Some(r#"'MANUAL'::"ObjectSource""#)]]).await;
    expect_rows(&pg, r#"SELECT e.enumlabel FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='DesignDocSize' ORDER BY e.enumsortorder"#,
        &[&[Some("SMALL")], &[Some("STANDARD")], &[Some("LARGE")]]).await;
    expect_rows(&pg, r#"SELECT conname, contype, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='"DesignDocVariationType"'::regclass AND contype='p'"#,
        &[&[Some("DesignDocVariationType_pkey"), Some("p"), Some("PRIMARY KEY (id)")]]).await;
    expect_rows(&pg, r#"SELECT i.relname, x.indisprimary, x.indisunique FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid WHERE x.indrelid='"DesignDocVariationType"'::regclass"#,
        &[&[Some("DesignDocVariationType_pkey"), Some("t"), Some("t")]]).await;

    drop(pg);
    f.stop().await;
}

#[path = "am2_fix1_network.inc"]
mod am2_fix1_network;
