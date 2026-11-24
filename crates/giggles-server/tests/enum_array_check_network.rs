use bytes::BytesMut;
use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, error::Error, sync::Arc, time::SystemTime};
use tokio_postgres::{
    NoTls,
    types::{IsNull, Kind, ToSql, Type, to_sql_checked},
};

#[derive(Debug)]
struct BinaryUuid(Option<[u8; 16]>);

impl ToSql for BinaryUuid {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        if let Some(bytes) = self.0 {
            out.extend_from_slice(&bytes);
            Ok(IsNull::No)
        } else {
            Ok(IsNull::Yes)
        }
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::UUID
    }

    to_sql_checked!();
}

#[tokio::test]
async fn migration_statements_and_prisma_extended_query() {
    let path = std::env::temp_dir().join(format!(
        "enum-array-check-network-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..Default::default()
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
        connections: 8,
    };
    let running = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);

    client.batch_execute(r#"
        CREATE TYPE "Size" AS ENUM ('SMALL', 'STANDARD', 'LARGE');
        CREATE TABLE vt (id int PRIMARY KEY, sizes "Size"[], counts jsonb);
        INSERT INTO vt (id, sizes) VALUES (1, '{SMALL,LARGE}'), (2, '{}'), (3, '{STANDARD}'), (4, '{LARGE}');
        CREATE TYPE "Kind" AS ENUM ('UPDATES', 'REPLACES', 'RELATES');
        CREATE TABLE rel (id int PRIMARY KEY, "toDocId" int, "toAdrId" int, "toAdrRevisionId" int, kind "Kind" NOT NULL);
        INSERT INTO rel VALUES (1, 10, NULL, NULL, 'UPDATES'), (2, 11, NULL, NULL, 'REPLACES');
        ALTER TABLE rel ADD COLUMN "toAdrRevisionAccepted" boolean NOT NULL DEFAULT false;
    "#).await.unwrap();
    for statement in [
        r#"ALTER TYPE "Kind" ADD VALUE 'IMPLEMENTS'"#,
        r#"ALTER TYPE "Kind" ADD VALUE 'RELIES_ON'"#,
        r#"ALTER TABLE rel ADD CONSTRAINT rel_target_check CHECK (
           ("toDocId" IS NOT NULL AND "toAdrId" IS NULL AND "toAdrRevisionId" IS NULL AND NOT "toAdrRevisionAccepted" AND kind::text IN ('UPDATES', 'REPLACES')) OR
           ("toDocId" IS NULL AND "toAdrId" IS NOT NULL AND kind::text IN ('IMPLEMENTS', 'RELIES_ON'))
        )"#,
    ] {
        client.batch_execute(statement).await.unwrap();
    }
    assert_eq!(
        client
            .execute(
                "UPDATE vt SET counts = jsonb_build_object(
            'SMALL', CASE WHEN 'SMALL' = ANY(sizes) THEN 1 ELSE 0 END,
            'STANDARD', CASE WHEN 'STANDARD' = ANY(sizes) THEN 1 ELSE 0 END,
            'LARGE', CASE WHEN 'LARGE' = ANY(sizes) THEN 1 ELSE 0 END
        )",
                &[],
            )
            .await
            .unwrap(),
        4
    );
    let counts: Vec<String> = client
        .query("SELECT counts::text FROM vt ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        counts,
        [
            r#"{"LARGE": 1, "SMALL": 1, "STANDARD": 0}"#,
            r#"{"LARGE": 0, "SMALL": 0, "STANDARD": 0}"#,
            r#"{"LARGE": 0, "SMALL": 0, "STANDARD": 1}"#,
            r#"{"LARGE": 1, "SMALL": 0, "STANDARD": 0}"#,
        ]
    );

    let definition: String = client.query_one(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'rel_target_check'",
        &[],
    ).await.unwrap().get(0);
    assert_eq!(
        definition,
        concat!(
            r#"CHECK (((("toDocId" IS NOT NULL) AND ("toAdrId" IS NULL) AND ("toAdrRevisionId" IS NULL) "#,
            r#"AND (NOT "toAdrRevisionAccepted") AND ((kind)::text = ANY (ARRAY['UPDATES'::text, 'REPLACES'::text]))) "#,
            r#"OR (("toDocId" IS NULL) AND ("toAdrId" IS NOT NULL) AND ((kind)::text = ANY (ARRAY['IMPLEMENTS'::text, 'RELIES_ON'::text])))))"#,
        )
    );
    let error = client
        .execute(
            "INSERT INTO rel VALUES (4, NULL, 7, NULL, 'UPDATES', false)",
            &[],
        )
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23514");
    assert_eq!(
        error.message(),
        r#"new row for relation "rel" violates check constraint "rel_target_check""#
    );
    assert_eq!(
        error.detail(),
        Some("Failing row contains (4, null, 7, null, UPDATES, f).")
    );

    let query = "SELECT x.v FROM (VALUES ('doc'::text), ('object'), ('adr')) x(v)
        WHERE $1::uuid IS NOT NULL AND ($2::uuid IS NULL OR true)
        AND $3 <> '' AND x.v = ANY($5::text[]) ORDER BY x.v LIMIT $4";
    let unspecified = Type::new("unspecified".into(), 0, Kind::Simple, "pg_catalog".into());
    let tags = vec!["doc", "object", "design-doc"];
    let org = "00000000-0000-0000-0000-0000000000aa";
    for viewer in [Type::UNKNOWN, unspecified.clone()] {
        let statement = client
            .prepare_typed(
                query,
                &[Type::TEXT, viewer, Type::TEXT, Type::INT8, Type::TEXT_ARRAY],
            )
            .await
            .unwrap();
        assert_eq!(
            statement.params(),
            [
                Type::TEXT,
                Type::UUID,
                Type::TEXT,
                Type::INT8,
                Type::TEXT_ARRAY
            ]
        );
        let rows = client
            .query(
                &statement,
                &[&org, &BinaryUuid(None), &"payment gateway", &10_i64, &tags],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.get::<_, &str>(0))
                .collect::<Vec<_>>(),
            ["doc", "object"]
        );
    }
    let unknown = client
        .prepare_typed(
            "SELECT pg_typeof($1)::text a, pg_typeof($2)::text b, pg_typeof($3)::text c, \
             pg_typeof($4)::text d, pg_typeof($5)::text e",
            &[
                Type::TEXT,
                Type::UNKNOWN,
                Type::TEXT,
                Type::INT8,
                Type::TEXT_ARRAY,
            ],
        )
        .await
        .unwrap_err();
    let unknown = unknown.as_db_error().unwrap();
    assert_eq!(unknown.code().code(), "42P18");
    assert_eq!(
        unknown.message(),
        "could not determine data type of parameter $2"
    );

    client
        .batch_execute(
            r#"
        CREATE TABLE sd (id int PRIMARY KEY, org uuid NOT NULL, kind text NOT NULL, title text);
        INSERT INTO sd VALUES
            (1, '00000000-0000-0000-0000-0000000000aa', 'doc', 'Billing gateway'),
            (2, '00000000-0000-0000-0000-0000000000aa', 'object', 'Payment service'),
            (3, '00000000-0000-0000-0000-0000000000aa', 'adr', 'Search');
    "#,
        )
        .await
        .unwrap();
    let cte = "WITH q AS (SELECT $2::text AS query) SELECT sd.title, q.query FROM sd, q
        WHERE sd.org = $1 AND sd.kind = ANY($4) ORDER BY sd.title LIMIT $3";
    let statement = client.prepare(cte).await.unwrap();
    assert_eq!(
        statement.params(),
        [Type::UUID, Type::TEXT, Type::INT8, Type::TEXT_ARRAY]
    );
    let mut org_bytes = [0; 16];
    org_bytes[15] = 0xaa;
    let organization = BinaryUuid(Some(org_bytes));
    let result = client
        .query(&statement, &[&organization, &"refunds", &10_i64, &tags])
        .await
        .unwrap();
    assert_eq!(
        result
            .iter()
            .map(|row| (row.get::<_, &str>(0), row.get::<_, &str>(1)))
            .collect::<Vec<_>>(),
        [
            ("Billing gateway", "refunds"),
            ("Payment service", "refunds")
        ]
    );
    let statement = client
        .prepare_typed(cte, &[Type::UUID, Type::TEXT, Type::INT4, Type::TEXT_ARRAY])
        .await
        .unwrap();
    assert_eq!(statement.params()[2], Type::INT4);
    let result = client
        .query(&statement, &[&organization, &"refunds", &1_i32, &tags])
        .await
        .unwrap();
    assert_eq!(result[0].get::<_, &str>(0), "Billing gateway");
    assert_eq!(result.len(), 1);

    for unresolved in [Type::UNKNOWN, unspecified] {
        let failure = client
            .prepare_typed(
                "SELECT jsonb_build_object($1, $2)",
                &[unresolved.clone(), unresolved],
            )
            .await
            .unwrap_err();
        let failure = failure.as_db_error().unwrap();
        assert_eq!(failure.code().code(), "42P18");
        assert_eq!(
            failure.message(),
            "could not determine data type of parameter $1"
        );
    }
    let failure = client
        .batch_execute("PREPARE pj AS SELECT jsonb_build_object($1, $2)")
        .await
        .unwrap_err();
    let failure = failure.as_db_error().unwrap();
    assert_eq!(failure.code().code(), "42P18");
    assert_eq!(
        failure.message(),
        "could not determine data type of parameter $1"
    );

    client
        .prepare("SELECT jsonb_build_object('a')")
        .await
        .unwrap();
    let failure = client
        .simple_query("SELECT jsonb_build_object('a', 1, 'b')")
        .await
        .unwrap_err();
    let failure = failure.as_db_error().unwrap();
    assert_eq!(failure.code().code(), "22023");
    assert_eq!(
        failure.message(),
        "argument list must have even number of elements"
    );
    assert_eq!(
        failure.hint(),
        Some("The arguments of jsonb_build_object() must consist of alternating keys and values.")
    );

    for sql in [
        "CREATE ROLE check_writer",
        "CREATE ROLE check_reader",
        r#"CREATE TABLE private_check (id int PRIMARY KEY, kind "Size", secret text DEFAULT 'topsecret',
               CONSTRAINT allowed_size CHECK (kind::text IN ('SMALL', 'STANDARD')))"#,
        "CREATE TABLE private_plain (id int PRIMARY KEY, n int, secret text DEFAULT 'topsecret',
               CONSTRAINT positive_n CHECK (n > 0))",
        "INSERT INTO private_check VALUES (1, 'SMALL', 'row1secret')",
        "INSERT INTO private_plain VALUES (1, 1, 'row1secret')",
        "GRANT INSERT, UPDATE ON private_check, private_plain TO check_writer",
        "GRANT INSERT, UPDATE, SELECT ON private_check TO check_reader",
        "SET ROLE check_writer",
    ] {
        client
            .batch_execute(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    for (sql, detail) in [
        (
            "INSERT INTO private_check (id, kind) VALUES (2, 'LARGE')",
            "Failing row contains (id, kind) = (2, LARGE).",
        ),
        (
            "INSERT INTO private_check VALUES (3, 'LARGE', 'mine')",
            "Failing row contains (id, kind, secret) = (3, LARGE, mine).",
        ),
        (
            "INSERT INTO private_plain (id, n) VALUES (2, 0)",
            "Failing row contains (id, n) = (2, 0).",
        ),
        (
            "UPDATE private_check SET kind = 'LARGE'",
            "Failing row contains (kind) = (LARGE).",
        ),
        (
            "UPDATE private_plain SET n = 0",
            "Failing row contains (n) = (0).",
        ),
    ] {
        let failure = client.simple_query(sql).await.unwrap_err();
        let failure = failure.as_db_error().unwrap();
        assert_eq!(failure.code().code(), "23514", "{sql}");
        assert_eq!(failure.detail(), Some(detail), "{sql}");
    }

    client.batch_execute("RESET ROLE").await.unwrap();
    client.batch_execute("SET ROLE check_reader").await.unwrap();
    let failure = client
        .simple_query("INSERT INTO private_check (id, kind) VALUES (5, 'LARGE')")
        .await
        .unwrap_err();
    let failure = failure.as_db_error().unwrap();
    assert_eq!(failure.code().code(), "23514");
    assert_eq!(
        failure.detail(),
        Some("Failing row contains (5, LARGE, topsecret).")
    );

    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
