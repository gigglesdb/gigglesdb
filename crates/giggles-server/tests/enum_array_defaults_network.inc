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

    let migration = concat!(r#"
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
CREATE TABLE "OAuthClient" ("id" UUID NOT NULL, "redirectUris" TEXT[], "grantTypes" TEXT[] DEFAULT ARRAY['authorization_code', 'refresh_token']::TEXT[], "label" VARCHAR(5) DEFAULT 'web', CONSTRAINT "OAuthClient_pkey" PRIMARY KEY ("id"));
INSERT INTO "OAuthClient" ("id", "redirectUris") VALUES ('00000000-0000-0000-0000-000000000001', '{https://example.org}');
CREATE TABLE "DesignDocVariationType" ("id" UUID NOT NULL, "presetKey" TEXT, "requiredFor" "DesignDocSize"[], "approvalFor" "DesignDocSize"[] DEFAULT ARRAY['LARGE']::"DesignDocSize"[], CONSTRAINT "DesignDocVariationType_pkey" PRIMARY KEY ("id"));
INSERT INTO "DesignDocVariationType" ("id", "presetKey", "requiredFor") VALUES ('00000000-0000-0000-0000-000000000001', 'evidence', '{STANDARD,SMALL}');
ALTER TABLE "DesignDocVariationType" ADD COLUMN "approvalCounts" JSONB NOT NULL DEFAULT '{"SMALL":0,"STANDARD":0,"LARGE":0}', ADD COLUMN "selfApproveAgentWork" BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE "DesignDocVariationType" ALTER COLUMN "approvalFor" SET DEFAULT ARRAY[]::"DesignDocSize"[];
UPDATE "DesignDocVariationType" SET "requiredFor" = ARRAY[]::"DesignDocSize"[] WHERE "presetKey" = 'evidence';
ALTER TABLE "OAuthClient" ADD COLUMN "excludedAccountIds" TEXT[] DEFAULT ARRAY[]::TEXT[], ADD COLUMN "orgExternalId" TEXT;
ALTER TABLE "OAuthClient" ADD COLUMN "linkDismissals" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[];
CREATE TYPE "DesignAuthorKind" AS ENUM ('HUMAN', 'AGENT', 'ARCHDOCS_AI');
"#,
        include_str!("../../giggles-core/tests/fixtures/archdocs_page_revision.sql"),
    );
    let completions = pg
        .simple_query(migration)
        .await
        .unwrap()
        .into_iter()
        .filter(|message| matches!(message, SimpleQueryMessage::CommandComplete(_)))
        .count();
    assert_eq!(completions, 19);

    assert_eq!(pg.execute(
        r#"INSERT INTO "Subscription" ("id", "organizationId") VALUES ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002')"#,
        &[],
    ).await.unwrap(), 1);
    pg.batch_execute(r#"
        INSERT INTO "PageRevision" ("pageId", "revision", "title", "content", "authorKind")
            VALUES ('00000000-0000-0000-0000-000000000001', 1, 'First', 'Body', 'HUMAN');
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
    expect_rows(&pg, r#"SELECT "redirectUris", "grantTypes", "excludedAccountIds", "linkDismissals", "orgExternalId", label FROM "OAuthClient""#,
        &[&[Some("{https://example.org}"), Some("{authorization_code,refresh_token}"), Some("{}"), Some("{}"), None, Some("web")]]).await;
    expect_rows(&pg, r#"SELECT "presetKey", "requiredFor", "approvalFor", "approvalCounts", "selfApproveAgentWork" FROM "DesignDocVariationType" ORDER BY "presetKey""#,
        &[
            &[Some("evidence"), Some("{}"), Some("{LARGE}"), Some(r#"{"LARGE": 0, "SMALL": 0, "STANDARD": 0}"#), Some("t")],
            &[Some("later"), Some("{LARGE,SMALL}"), Some("{}"), Some(r#"{"LARGE": 0, "SMALL": 0, "STANDARD": 0}"#), Some("t")],
        ]).await;


    expect_rows(
        &pg,
        r#"SELECT pg_typeof(sizes), sizes FROM (
            SELECT greatest(coalesce("requiredFor", '{SMALL}'), '{STANDARD}') AS sizes
            FROM "DesignDocVariationType"
            UNION ALL SELECT '{SMALL}'
        ) s ORDER BY sizes"#,
        &[
            &[Some(r#""DesignDocSize"[]"#), Some("{SMALL}")],
            &[Some(r#""DesignDocSize"[]"#), Some("{STANDARD}")],
            &[Some(r#""DesignDocSize"[]"#), Some("{LARGE,SMALL}")],
        ],
    ).await;

    expect_rows(
        &pg,
        r#"SELECT "authorUserIds", "id" IS NOT NULL, "createdAt" IS NOT NULL FROM "PageRevision""#,
        &[&[Some("{}"), Some("t"), Some("t")]],
    ).await;
    expect_rows(
        &pg,
        r#"SELECT pg_get_expr(d.adbin,d.adrelid) FROM pg_attrdef d
           JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum
           WHERE d.adrelid='"PageRevision"'::regclass AND a.attname='authorUserIds'"#,
        &[&[Some("ARRAY[]::uuid[]")]],
    ).await;
    expect_rows(
        &pg,
        "SELECT column_default, is_nullable, data_type, udt_name FROM information_schema.columns
         WHERE table_name='PageRevision' AND column_name='authorUserIds'",
        &[&[Some("ARRAY[]::uuid[]"), Some("YES"), Some("ARRAY"), Some("_uuid")]],
    ).await;

    for (sql, code, message, hint) in [
        (
            "CREATE TABLE pr_bad (id int PRIMARY KEY, u uuid[] DEFAULT '{nope}')",
            "22P02",
            r#"invalid input syntax for type uuid: "nope""#,
            None,
        ),
        (
            "CREATE TABLE pr_bad2 (id int PRIMARY KEY, u uuid[] DEFAULT ARRAY[]::text[])",
            "42804",
            r#"column "u" is of type uuid[] but default expression is of type text[]"#,
            Some("You will need to rewrite or cast the expression."),
        ),
        (
            "CREATE TABLE pr_bad3 (id int PRIMARY KEY, u uuid[] DEFAULT ARRAY[1])",
            "42804",
            r#"column "u" is of type uuid[] but default expression is of type integer[]"#,
            Some("You will need to rewrite or cast the expression."),
        ),
    ] {
        let error = pg.simple_query(sql).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), code, "{sql}");
        assert_eq!(error.message(), message, "{sql}");
        assert_eq!(error.hint(), hint, "{sql}");
    }

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
            r#"SELECT '{NOPE}' = "requiredFor" FROM "DesignDocVariationType" WHERE false"#,
            "22P02",
            r#"invalid input value for enum "DesignDocSize": "NOPE""#,
        ),
        (
            r#"SELECT "requiredFor" FROM "DesignDocVariationType" UNION ALL SELECT ARRAY['MANUAL']::"ObjectSource"[]"#,
            "42846",
            r#"UNION could not convert type "ObjectSource"[] to "DesignDocSize"[]"#,
        ),
        (
            r#"SELECT CASE WHEN false THEN "requiredFor" ELSE 'SMALL'::text END FROM "DesignDocVariationType""#,
            "42804",
            r#"CASE types text and "DesignDocSize"[] cannot be matched"#,
        ),
        (
            r#"SELECT lag("requiredFor",1,NULL::"ObjectSource"[]) OVER (ORDER BY id) FROM "DesignDocVariationType""#,
            "42883",
            r#"function lag("DesignDocSize"[], integer, "ObjectSource"[]) does not exist"#,
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
            &[Some("label"), Some("'web'::text")],
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

use tokio_postgres::types::{FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};

#[derive(Debug, PartialEq)]
struct EnumLabel(String);

impl ToSql for EnumLabel {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Enum(_))
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for EnumLabel {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(std::str::from_utf8(raw)?.into()))
    }

    fn accepts(ty: &Type) -> bool {
        <Self as ToSql>::accepts(ty)
    }
}

#[tokio::test]
async fn prisma_prepares_enum_parameters_without_oids() {
    let engine = if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    };
    let fixture = Fixture::with_engine(engine).await;
    let pg = fixture.client().await;
    pg.batch_execute(
        r#"
        CREATE TYPE "Status" AS ENUM ('TRIALING','ACTIVE','CANCELED');
        CREATE TYPE pe AS ENUM ('z','a');
        CREATE TABLE "Sub"(id int PRIMARY KEY, "status" "Status" NOT NULL DEFAULT 'TRIALING',
            tags "Status"[] DEFAULT ARRAY[]::"Status"[], a pe[], v pe);
        INSERT INTO "Sub" VALUES (1,'ACTIVE','{ACTIVE}','{z}','z'),(2,'CANCELED',NULL,NULL,NULL);
    "#,
    )
    .await
    .unwrap();

    let mut types = std::collections::BTreeMap::new();
    for row in rows(
        &pg,
        "SELECT typname, oid, typarray FROM pg_type WHERE typname IN ('pe','Status')",
    )
    .await
    {
        let name = row[0].as_deref().unwrap();
        types.insert(
            name.to_owned(),
            row[1].as_deref().unwrap().parse::<u32>().unwrap(),
        );
        types.insert(
            format!("_{name}"),
            row[2].as_deref().unwrap().parse::<u32>().unwrap(),
        );
    }
    types.insert("int4".into(), Type::INT4.oid());

    let mut plans = Vec::new();
    for (query, names) in [
        (r#"SELECT coalesce(a,$1) FROM "Sub""#, &["_pe"][..]),
        (
            r#"SELECT coalesce(v,$1), CASE WHEN id=1 THEN v ELSE $2 END, nullif(v,$3),
            greatest(v,$4), lag(v,1,$5) OVER (ORDER BY id) FROM "Sub""#,
            &["pe"; 5],
        ),
        (r#"SELECT id FROM "Sub" WHERE "status" = $1"#, &["Status"]),
        (
            r#"SELECT id FROM "Sub" WHERE "status" = ANY($1)"#,
            &["_Status"],
        ),
        (
            r#"UPDATE "Sub" SET "status" = $1, tags = $2 WHERE id = $3"#,
            &["Status", "_Status", "int4"],
        ),
        (
            r#"INSERT INTO "Sub"(id,"status",tags) VALUES ($1,$2,$3)"#,
            &["int4", "Status", "_Status"],
        ),
    ] {
        let plan = pg
            .prepare(query)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error:?}"));
        let params: Vec<_> = plan
            .params()
            .iter()
            .map(|ty| (ty.name(), ty.oid()))
            .collect();
        assert_eq!(
            params,
            names
                .iter()
                .map(|name| (*name, types[*name]))
                .collect::<Vec<_>>(),
            "{query}"
        );
        plans.push(plan);
    }

    let label = |text: &str| EnumLabel(text.into());
    let array = |labels: &[&str]| labels.iter().map(|text| label(text)).collect::<Vec<_>>();
    let result = pg.query(&plans[0], &[&array(&["a"])]).await.unwrap();
    assert_eq!(
        result
            .iter()
            .map(|row| row.get::<_, Vec<EnumLabel>>(0))
            .collect::<Vec<_>>(),
        [array(&["z"]), array(&["a"])]
    );
    let result = pg
        .query(
            &plans[1],
            &[
                &label("a"),
                &label("a"),
                &label("z"),
                &label("a"),
                &label("a"),
            ],
        )
        .await
        .unwrap();
    let actual: Vec<_> = result
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.get::<_, Option<EnumLabel>>(i))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        actual,
        [
            vec![
                Some(label("z")),
                Some(label("z")),
                None,
                Some(label("a")),
                Some(label("a"))
            ],
            vec![
                Some(label("a")),
                Some(label("a")),
                None,
                Some(label("a")),
                Some(label("z"))
            ],
        ]
    );
    assert_eq!(
        pg.query_one(&plans[2], &[&label("ACTIVE")])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    let result = pg
        .query(&plans[3], &[&array(&["ACTIVE", "CANCELED"])])
        .await
        .unwrap();
    assert_eq!(
        result
            .iter()
            .map(|row| row.get::<_, i32>(0))
            .collect::<Vec<_>>(),
        [1, 2]
    );

    for (plan, values, name) in [
        (
            &plans[2],
            &[&label("BOGUS") as &(dyn ToSql + Sync)][..],
            r#""Status""#,
        ),
        (
            &plans[0],
            &[&array(&["BOGUS"]) as &(dyn ToSql + Sync)][..],
            "pe",
        ),
    ] {
        let error = pg.query(plan, values).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "22P02");
        assert_eq!(
            error.message(),
            format!("invalid input value for enum {name}: \"BOGUS\"")
        );
    }

    assert_eq!(
        pg.execute(
            &plans[4],
            &[&label("CANCELED"), &array(&["CANCELED", "ACTIVE"]), &1i32]
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        pg.execute(&plans[5], &[&3i32, &label("TRIALING"), &array(&[])])
            .await
            .unwrap(),
        1
    );
    expect_rows(
        &pg,
        r#"SELECT id,"status",tags FROM "Sub" ORDER BY id"#,
        &[
            &[Some("1"), Some("CANCELED"), Some("{CANCELED,ACTIVE}")],
            &[Some("2"), Some("CANCELED"), None],
            &[Some("3"), Some("TRIALING"), Some("{}")],
        ],
    )
    .await;
}


#[derive(Debug)]
struct BinaryEnumArray<'a> {
    element_oid: u32,
    labels: &'a [&'a [u8]],
}

impl ToSql for BinaryEnumArray<'_> {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(&1i32.to_be_bytes()); // dimensions
        out.extend_from_slice(&0i32.to_be_bytes()); // no nulls
        out.extend_from_slice(&self.element_oid.to_be_bytes());
        out.extend_from_slice(&i32::try_from(self.labels.len())?.to_be_bytes());
        out.extend_from_slice(&1i32.to_be_bytes()); // lower bound
        for label in self.labels {
            out.extend_from_slice(&i32::try_from(label.len())?.to_be_bytes());
            out.extend_from_slice(label);
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Array(element) if <EnumLabel as ToSql>::accepts(element))
    }

    to_sql_checked!();
}

#[tokio::test]
async fn inferred_enum_array_binary_bind_ignores_header_element_oid() {
    let engine = if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    };
    let fixture = Fixture::with_engine(engine).await;
    let pg = fixture.client().await;
    pg.batch_execute(
        "CREATE TYPE qe AS ENUM ('z','a');
         CREATE TABLE q(id int PRIMARY KEY, a qe[]);
         INSERT INTO q VALUES (1,'{z}'),(2,'{a,z}');",
    )
    .await
    .unwrap();

    let plan = pg.prepare("SELECT id FROM q WHERE a = $1").await.unwrap();
    let Kind::Array(element) = plan.params()[0].kind() else {
        panic!("expected inferred enum-array parameter, got {:?}", plan.params());
    };
    assert_eq!(element.name(), "qe");
    for element_oid in [element.oid(), Type::TEXT.oid(), Type::VARCHAR.oid(), Type::INT4.oid(), 0] {
        let valid = BinaryEnumArray {
            element_oid,
            labels: &[b"a", b"z"],
        };
        let row = pg.query_one(&plan, &[&valid]).await.unwrap();
        assert_eq!(row.get::<_, i32>(0), 2, "element OID {element_oid}");

        let invalid = BinaryEnumArray {
            element_oid,
            labels: &[b"BOGUS"],
        };
        let error = pg.query(&plan, &[&invalid]).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "22P02", "element OID {element_oid}");
        assert_eq!(error.message(), r#"invalid input value for enum qe: "BOGUS""#);
    }
    for (label, hex) in [(b"\0\0\0\x01".as_slice(), "0x00"), (b"\xff".as_slice(), "0xff")] {
        let invalid = BinaryEnumArray {
            element_oid: Type::INT4.oid(),
            labels: &[label],
        };
        let error = pg.query(&plan, &[&invalid]).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "22021");
        assert_eq!(error.message(), format!(r#"invalid byte sequence for encoding "UTF8": {hex}"#));
    }
    expect_rows(
        &pg,
        "SELECT id, a FROM q ORDER BY id",
        &[&[Some("1"), Some("{z}")], &[Some("2"), Some("{a,z}")]],
    )
    .await;
    drop(pg);
    fixture.stop().await;
}

#[tokio::test]
async fn planned_enum_insert_uses_postgres_assignment_diagnostics() {
    let engine = if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    };
    let fixture = Fixture::with_engine(engine).await;
    let pg = fixture.client().await;
    pg.batch_execute(
        "CREATE TYPE mood AS ENUM ('z', 'a', 'm');
         CREATE TABLE enum_overlap (id int PRIMARY KEY, feeling mood, feelings mood[]);
         INSERT INTO enum_overlap
         SELECT 1, v.feeling, v.feelings
         FROM (VALUES ('a'::mood, ARRAY['a']::mood[])) AS v(feeling, feelings)",
    )
    .await
    .unwrap();
    expect_rows(
        &pg,
        "SELECT feeling, feelings FROM enum_overlap",
        &[&[Some("a"), Some("{a}")]],
    )
    .await;
    let error = pg
        .prepare("INSERT INTO enum_overlap (id, feeling) SELECT 2, v.x FROM (VALUES ('a')) AS v(x)")
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "42804");
    assert_eq!(
        error.message(),
        r#"column "feeling" is of type mood but expression is of type text"#,
    );
    assert_eq!(
        error.hint(),
        Some("You will need to rewrite or cast the expression."),
    );
}
