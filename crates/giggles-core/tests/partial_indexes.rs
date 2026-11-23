mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlResult, Value, take_sql_error_detail, take_sql_error_message},
};

fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
}

fn fails(s: &mut SqlSession, sql: &str, code: &str, message: Option<&str>, detail: Option<&str>) {
    let err = s.execute(sql, &[], None).expect_err(sql);
    assert_eq!(giggles_core::sqlstate(&err), code, "{sql}: {err:?}");
    let actual_message = take_sql_error_message();
    if let Some(expected) = message {
        assert_eq!(actual_message.as_deref(), Some(expected), "{sql}");
    }
    assert_eq!(take_sql_error_detail().as_deref(), detail, "{sql}");
}

fn with_engine(engine: Engine, test: impl FnOnce(&Admin, &std::path::Path)) {
    let path = common::fresh("partial-indexes");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    test(&admin, &path);
    drop(admin);
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

fn migration(s: &mut SqlSession) {
    for sql in [
        "CREATE TYPE st AS ENUM ('OPEN','CLOSED','QUEUED','CREATED')",
        r#"CREATE TABLE "Am" (id int PRIMARY KEY, g int, kind text, status st, "isDefault" boolean, "deletedAt" timestamp(3), "dedupeKey" text)"#,
        r#"CREATE UNIQUE INDEX "Am_one_open" ON "Am"(g) WHERE status = 'OPEN'"#,
        r#"CREATE UNIQUE INDEX "Am_live_default" ON "Am"(g) WHERE "isDefault" AND "deletedAt" IS NULL"#,
        r#"CREATE INDEX "Am_pending" ON "Am"("dedupeKey") WHERE status IN ('QUEUED', 'CREATED')"#,
        r#"CREATE INDEX "Am_nn" ON "Am"(g) WHERE "Am"."dedupeKey" IS NOT NULL AND kind IS NOT NULL"#,
        r#"INSERT INTO "Am" (id,g,status,"isDefault",kind) VALUES (1,1,'OPEN',true,'a'),(2,1,'CLOSED',false,'b'),(3,1,NULL,NULL,'c')"#,
    ] {
        run(s, sql);
    }
}

fn exercise(engine: Engine) {
    let path = common::fresh("partial-index-reopen");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        migration(&mut s);
        fails(
            &mut s,
            r#"INSERT INTO "Am"(id,g,status) VALUES (4,1,'OPEN')"#,
            "23505",
            Some(r#"duplicate key value violates unique constraint "Am_one_open""#),
            Some("Key (g)=(1) already exists."),
        );
        fails(
            &mut s,
            r#"UPDATE "Am" SET status='OPEN' WHERE id=2"#,
            "23505",
            None,
            Some("Key (g)=(1) already exists."),
        );
        run(&mut s, r#"UPDATE "Am" SET status='CLOSED' WHERE id=1"#);
        run(&mut s, r#"UPDATE "Am" SET status='OPEN', g=2 WHERE id=2"#);
        run(&mut s, r#"INSERT INTO "Am"(id,g,status) SELECT 4,1,'OPEN'"#);
        assert_eq!(
            run(&mut s, r#"SELECT id FROM "Am" WHERE g=1 ORDER BY id"#)
                .rows
                .iter()
                .map(|r| r.values[0].clone())
                .collect::<Vec<_>>(),
            [Value::Int(1), Value::Int(3), Value::Int(4)]
        );
        for sql in [
            "BEGIN",
            "SAVEPOINT sp",
            r#"DELETE FROM "Am" WHERE id=4"#,
            r#"INSERT INTO "Am"(id,g,status) VALUES (5,1,'OPEN')"#,
            "ROLLBACK TO SAVEPOINT sp",
            "COMMIT",
            "BEGIN",
            r#"UPDATE "Am" SET g=3 WHERE id=4"#,
            "ROLLBACK",
        ] {
            run(&mut s, sql);
        }
        assert_eq!(
            run(&mut s, r#"SELECT id,g FROM "Am" WHERE id=4"#).rows[0].values,
            [Value::Int(4), Value::Int(1)]
        );
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        let insert = r#"INSERT INTO "Am"(id,g,status,"isDefault",kind) VALUES (6,1,'CLOSED',true,'updated')"#;
        let expectation =
            "there is no unique or exclusion constraint matching the ON CONFLICT specification";
        for target in ["(g)", r#"(g) WHERE "deletedAt" IS NULL"#] {
            fails(
                &mut s,
                &format!("{insert} ON CONFLICT {target} DO NOTHING"),
                "42P10",
                Some(expectation),
                None,
            );
        }
        let returning = run(
            &mut s,
            &format!(
                r#"{insert} ON CONFLICT (g) WHERE "deletedAt" IS NULL AND "isDefault" AND kind IS NULL DO UPDATE SET kind=excluded.kind RETURNING id,kind"#
            ),
        );
        assert_eq!(
            returning.rows[0].values,
            [Value::Int(1), Value::Text("updated".into())]
        );
        fails(
            &mut s,
            &format!(r#"{insert} ON CONFLICT ON CONSTRAINT "Am_live_default" DO NOTHING"#),
            "42704",
            Some(r#"constraint "Am_live_default" for table "Am" does not exist"#),
            None,
        );

        let outside = run(
            &mut s,
            r#"INSERT INTO "Am"(id,g,status,"isDefault") VALUES (10,1,'CLOSED',false)
            ON CONFLICT (g) WHERE "isDefault" AND "deletedAt" IS NULL DO NOTHING RETURNING id"#,
        );
        assert_eq!(outside.rows[0].values, [Value::Int(10)]);

        let (_, copy) = s
            .prepare_copy(r#"COPY "Am"(id,g,status) FROM STDIN"#)
            .unwrap();
        let text = |v: &str| Value::Text(v.into());
        assert_eq!(
            s.ingest_copy(&copy, vec![vec![text("7"), text("1"), text("CLOSED")]])
                .unwrap()
                .affected,
            1
        );
        let err = s
            .ingest_copy(&copy, vec![vec![text("8"), text("1"), text("OPEN")]])
            .unwrap_err();
        assert_eq!(giggles_core::sqlstate(&err), "23505");
        take_sql_error_detail();
        take_sql_error_message();

        run(
            &mut s,
            r#"ALTER TABLE "Am" RENAME COLUMN "deletedAt" TO "archivedAt""#,
        );
        let (_, indexes) = admin.secondary_index_catalog().unwrap();
        let predicate = |name: &str| {
            indexes
                .iter()
                .find(|i| i.name == name)
                .unwrap()
                .predicate
                .as_deref()
                .unwrap()
        };
        assert_eq!(
            predicate("Am_live_default"),
            r#"("isDefault" AND ("archivedAt" IS NULL))"#
        );
        assert_eq!(
            predicate("Am_pending"),
            "(status = ANY (ARRAY['QUEUED'::st, 'CREATED'::st]))"
        );
        assert_eq!(predicate("Am_one_open"), "(status = 'OPEN'::st)");
        fails(
            &mut s,
            r#"ALTER TABLE "Am" ALTER COLUMN "archivedAt" TYPE date"#,
            "0A000",
            None,
            None,
        );
        run(&mut s, r#"ALTER TABLE "Am" DROP COLUMN "archivedAt""#);
        assert!(
            !admin
                .secondary_index_catalog()
                .unwrap()
                .1
                .iter()
                .any(|i| i.name == "Am_live_default")
        );
        run(&mut s, r#"DROP INDEX "Am_one_open""#);
        run(
            &mut s,
            r#"INSERT INTO "Am"(id,g,status) VALUES (9,1,'OPEN')"#,
        );
        run(&mut s, r#"DROP TABLE "Am""#);
        assert!(admin.secondary_index_catalog().unwrap().1.is_empty());
    }
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

fn validations(engine: Engine) {
    with_engine(engine, |admin, _| {
        let mut s = admin.sql_session();
        run(
            &mut s,
            r#"CREATE TABLE "Am" (id int PRIMARY KEY, n int, f boolean, dt timestamp)"#,
        );
        for (predicate, code, message) in [
            (
                "nosuch IS NULL",
                "42703",
                r#"column "nosuch" does not exist"#,
            ),
            (
                "random() > 0.5",
                "42P17",
                "functions in index predicate must be marked IMMUTABLE",
            ),
            (
                "count(*) > 1",
                "42803",
                "aggregate functions are not allowed in index predicates",
            ),
            (
                "n IN (SELECT 1)",
                "0A000",
                "cannot use subquery in index predicate",
            ),
            (
                "n",
                "42804",
                "argument of WHERE must be type boolean, not type integer",
            ),
            (
                "now() > dt",
                "42P17",
                "functions in index predicate must be marked IMMUTABLE",
            ),
            (
                "other.n > 1",
                "42P01",
                r#"missing FROM-clause entry for table "other""#,
            ),
        ] {
            fails(
                &mut s,
                &format!(r#"CREATE INDEX i ON "Am"(n) WHERE {predicate}"#),
                code,
                Some(message),
                None,
            );
        }
        run(&mut s, r#"CREATE INDEX i ON "Am"(n) WHERE "Am".n > 1"#);
        run(
            &mut s,
            r#"INSERT INTO "Am" VALUES (1,1,true,NULL),(2,1,false,NULL),(3,1,false,NULL),(4,1,NULL,NULL)"#,
        );
        run(&mut s, r#"CREATE UNIQUE INDEX dup_ok ON "Am"(n) WHERE f"#);
        fails(
            &mut s,
            r#"CREATE UNIQUE INDEX dup_bad ON "Am"(n) WHERE NOT f"#,
            "23505",
            Some(r#"could not create unique index "dup_bad""#),
            Some("Key (n)=(1) is duplicated."),
        );
        run(
            &mut s,
            r#"CREATE UNIQUE INDEX dup_null ON "Am"(n) WHERE f IS NULL"#,
        );
        let err = s
            .execute(
                r#"ALTER TABLE "Am" ADD CONSTRAINT dc UNIQUE (n) WHERE f"#,
                &[],
                None,
            )
            .unwrap_err();
        assert_eq!(
            err,
            CoreError::Sql(giggles_core::relational::SqlError::Syntax)
        );
        run(&mut s, r#"ALTER TABLE "Am" DROP COLUMN f"#);
        fails(
            &mut s,
            r#"ALTER TABLE "Am" ALTER COLUMN n TYPE bigint"#,
            "0A000",
            None,
            None,
        );
        let (_, indexes) = admin.secondary_index_catalog().unwrap();
        assert_eq!(
            indexes.iter().map(|i| i.name.as_str()).collect::<Vec<_>>(),
            ["i"]
        );
        for sql in [
            "CREATE TABLE smoke(id int PRIMARY KEY, g int, f boolean, label text)",
            "CREATE UNIQUE INDEX smoke_p ON smoke(g) WHERE f",
            "CREATE UNIQUE INDEX smoke_g ON smoke(g)",
            "INSERT INTO smoke VALUES (1,1,false,'old')",
        ] {
            run(&mut s, sql);
        }
        let result = run(
            &mut s,
            "INSERT INTO smoke VALUES (2,1,false,'new') ON CONFLICT (g) WHERE f DO UPDATE SET label=excluded.label RETURNING id,label",
        );
        assert_eq!(
            result.rows[0].values,
            [Value::Int(1), Value::Text("new".into())]
        );

        for sql in [
            "CREATE TABLE cast_pred(id int PRIMARY KEY, n int, marker varchar(64))",
            "INSERT INTO cast_pred VALUES (1,1,'live'), (2,1,NULL)",
            "CREATE UNIQUE INDEX cast_pred_live ON cast_pred(n) WHERE marker IS NOT NULL",
            "ALTER TABLE cast_pred ALTER COLUMN marker TYPE text",
        ] {
            run(&mut s, sql);
        }
        fails(
            &mut s,
            "INSERT INTO cast_pred VALUES (3,1,'duplicate')",
            "23505",
            Some(r#"duplicate key value violates unique constraint "cast_pred_live""#),
            Some("Key (n)=(1) already exists."),
        );
    });
}

#[test]
fn legacy_lifecycle() {
    exercise(Engine::Legacy);
}
#[test]
fn legacy_validation() {
    validations(Engine::Legacy);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_lifecycle() {
    exercise(Engine::V2);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_validation() {
    validations(Engine::V2);
}

fn review_reads(engine: Engine) {
    with_engine(engine, |admin, _| {
        let mut s = admin.sql_session();
        for sql in [
            "CREATE TABLE a (id int PRIMARY KEY, g int)",
            "CREATE TABLE b (id int PRIMARY KEY, g int, f boolean)",
            "CREATE UNIQUE INDEX b_g ON b(g) WHERE f",
            "INSERT INTO a VALUES (1,10),(2,20),(3,30)",
            "INSERT INTO b VALUES (1,10,true),(2,20,false),(3,30,NULL),(4,10,false)",
            r#"CREATE TABLE docs (id int PRIMARY KEY, "createdAt" timestamp, "deletedAt" timestamp)"#,
            r#"CREATE INDEX live_docs ON docs("createdAt") WHERE "deletedAt" IS NULL"#,
            r#"INSERT INTO docs VALUES (1,'2024-02-01',NULL),(2,'2024-02-01','2024-02-02'),(3,'2023-01-01',NULL)"#,
        ] {
            run(&mut s, sql);
        }
        let rows = |s: &mut SqlSession, sql: &str| {
            run(s, sql)
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>()
        };
        let expected = |rows: &[&[i64]]| {
            rows.iter()
                .map(|row| row.iter().copied().map(Value::Int).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        };
        for join in ["JOIN", "LEFT JOIN"] {
            assert_eq!(
                rows(
                    &mut s,
                    &format!("SELECT a.id,b.id FROM a {join} b ON b.g=a.g ORDER BY 1,2")
                ),
                expected(&[&[1, 1], &[1, 4], &[2, 2], &[3, 3]]),
            );
        }
        assert_eq!(
            rows(
                &mut s,
                "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.g=a.g) ORDER BY id"
            ),
            expected(&[&[1], &[2], &[3]]),
        );
        assert!(
            rows(
                &mut s,
                "SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.g=a.g)"
            )
            .is_empty()
        );
        let range = r#"SELECT id FROM docs WHERE "createdAt" > '2024-01-01' ORDER BY id"#;
        assert_eq!(rows(&mut s, range), expected(&[&[1], &[2]]));
        let point = s
            .prepare("SELECT id FROM b WHERE g=20 ORDER BY id")
            .unwrap();
        let mut cursor = s.open_cursor(&point, &[]).unwrap();
        assert_eq!(cursor.fetch(1, 1024).unwrap()[0].values, [Value::Int(2)]);
        use giggles_core::operations::{
            Bindings, Compare, Expr, ReadExplainStage, Request, ValueExpr,
        };
        let request = Request::Read {
            table: "b".into(),
            filter: Some(Expr::Compare(
                Box::new(Expr::Field("g".into())),
                Compare::Eq,
                Box::new(Expr::Value(ValueExpr::Literal(Value::Int(20)))),
            )),
            order: vec![],
            limit: None,
            projection: vec![],
        };
        let explain = giggles_core::operations::Engine::new(admin.clone());
        assert!(
            !explain
                .explain_read(&request, &Bindings::new())
                .unwrap()
                .stages
                .iter()
                .any(|stage| matches!(stage, ReadExplainStage::SecondaryIndexCandidates { .. }))
        );
    });
}

fn review_predicates(engine: Engine) {
    let path = common::fresh("partial-index-review");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        run(
            &mut s,
            "CREATE TABLE r (id int PRIMARY KEY, d timestamp, g int, k text, v varchar(20))",
        );
        for (predicate, code, message) in [
            (
                "d > '2024-01-01'::timestamp with time zone",
                "42P17",
                "functions in index predicate must be marked IMMUTABLE",
            ),
            (
                "'abc'",
                "22P02",
                r#"invalid input syntax for type boolean: "abc""#,
            ),
            ("g > $1", "42P02", "there is no parameter $1"),
        ] {
            fails(
                &mut s,
                &format!("CREATE INDEX bad ON r(g) WHERE {predicate}"),
                code,
                Some(message),
                None,
            );
        }
        for sql in [
            "INSERT INTO r VALUES (1,'2024-02-01',100,'aye','x'),(2,'2023-01-01',100,'boo','y')",
            "CREATE INDEX implicit_date ON r(g) WHERE d > '2024-01-01'",
            "CREATE INDEX explicit_date ON r(g) WHERE d > '2024-01-01'::timestamp without time zone",
            "CREATE INDEX varchar_match ON r(g) WHERE v = 'x'",
            "CREATE UNIQUE INDEX like_match ON r(g) WHERE k LIKE 'a%'",
            "CREATE UNIQUE INDEX lower_match ON r(g) WHERE lower(k) = 'a'",
        ] {
            run(&mut s, sql);
        }
        use giggles_pg_types::{
            bytea::ByteaOutput,
            temporal::{DateOrder, DateStyle, DateStyleSettings},
        };
        s.set_text_settings(
            DateStyleSettings {
                style: DateStyle::Iso,
                order: DateOrder::Dmy,
            },
            ByteaOutput::default(),
        );
        for predicate in ["d > '31/01/2024'", "d > '31/01/2024'::timestamp"] {
            fails(
                &mut s,
                &format!("CREATE INDEX unstable_date ON r(g) WHERE {predicate}"),
                "0A000",
                None,
                None,
            );
        }
        s.set_text_settings(DateStyleSettings::default(), ByteaOutput::default());
        let conjunction = |terms| {
            (1..=terms)
                .map(|i| format!("g <> {i}"))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        fails(
            &mut s,
            &format!("CREATE INDEX too_deep ON r(g) WHERE {}", conjunction(75)),
            "54000",
            None,
            None,
        );
        fails(
            &mut s,
            &format!("CREATE INDEX too_deep ON r(g) WHERE {}", conjunction(32)),
            "54000",
            None,
            None,
        );
        fails(
            &mut s,
            &format!(
                "INSERT INTO r(id,g) VALUES (3,100) ON CONFLICT (g) WHERE {} DO NOTHING",
                conjunction(75)
            ),
            "54000",
            None,
            None,
        );
        run(
            &mut s,
            &format!(
                "CREATE UNIQUE INDEX bounded ON r(g) WHERE {}",
                conjunction(31).replace("g <> 31", "k IS NULL")
            ),
        );
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        let indexes = admin.secondary_index_catalog().unwrap().1;
        for (name, predicate) in [
            (
                "implicit_date",
                "(d > '2024-01-01 00:00:00'::timestamp without time zone)",
            ),
            (
                "explicit_date",
                "(d > '2024-01-01 00:00:00'::timestamp without time zone)",
            ),
            ("varchar_match", "((v)::text = 'x'::text)"),
            ("like_match", "(k ~~ 'a%'::text)"),
            ("lower_match", "(lower(k) = 'a'::text)"),
        ] {
            assert_eq!(
                indexes
                    .iter()
                    .find(|index| index.name == name)
                    .unwrap()
                    .predicate
                    .as_deref(),
                Some(predicate)
            );
        }
        run(
            &mut s,
            "INSERT INTO r(id,g,k) VALUES (3,101,'A'),(4,100,'beta'),(5,103,NULL)",
        );
        for (sql, name, g) in [
            (
                "INSERT INTO r(id,g,k) VALUES (6,100,'acid')",
                "like_match",
                100,
            ),
            (
                "INSERT INTO r(id,g,k) VALUES (6,101,'a')",
                "lower_match",
                101,
            ),
            ("INSERT INTO r(id,g) VALUES (6,103)", "bounded", 103),
        ] {
            fails(
                &mut s,
                sql,
                "23505",
                Some(&format!(
                    "duplicate key value violates unique constraint \"{name}\""
                )),
                Some(&format!("Key (g)=({g}) already exists.")),
            );
        }
    }
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn legacy_review_reads() {
    review_reads(Engine::Legacy);
}

#[test]
fn legacy_review_predicates() {
    review_predicates(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_review_reads() {
    review_reads(Engine::V2);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_review_predicates() {
    review_predicates(Engine::V2);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_review_view_column_names() {
    with_engine(Engine::V2, |admin, _| {
        let mut s = admin.sql_session();
        for sql in [
            "CREATE TABLE nm (id int PRIMARY KEY, approvals int, system int)",
            "INSERT INTO nm VALUES (1,11,21)",
            "CREATE VIEW v AS SELECT approvals, system FROM nm",
            "CREATE VIEW aliased AS SELECT id AS approvals, system FROM nm",
        ] {
            run(&mut s, sql);
        }
        assert_eq!(
            run(&mut s, "SELECT approvals, system FROM v").rows[0].values,
            [Value::Int(11), Value::Int(21)]
        );
        assert_eq!(
            run(&mut s, "SELECT approvals, system FROM aliased").rows[0].values,
            [Value::Int(1), Value::Int(21)]
        );
        fails(
            &mut s,
            "CREATE VIEW approvals AS SELECT id FROM nm",
            "0A000",
            None,
            None,
        );
    });
}

fn wide_document_migration(engine: Engine) {
    let path = common::fresh("wide-document-migration");
    let text = |value: &str| Value::Text(value.into());
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut session = admin.sql_session();
        for sql in [
            r#"BEGIN"#,
            r#"CREATE TABLE "Document" (id int PRIMARY KEY, "organizationId" uuid NOT NULL, "externalId" uuid NOT NULL,
                "createdAt" timestamp(3) NOT NULL, "updatedAt" timestamptz, "deletedAt" timestamp(3),
                "reviewDate" date, "projectId" uuid, "reviewerIds" uuid[], state text, title text)"#,
            r#"CREATE INDEX "Document_organizationId_idx" ON "Document"("organizationId")"#,
            r#"CREATE INDEX "Document_createdAt_idx" ON "Document"("createdAt")"#,
            r#"CREATE INDEX "Document_updatedAt_idx" ON "Document"("updatedAt")"#,
            r#"CREATE INDEX "Document_deletedAt_idx" ON "Document"("deletedAt")"#,
            r#"CREATE INDEX "Document_reviewDate_idx" ON "Document"("reviewDate")"#,
            r#"CREATE INDEX "Document_projectId_idx" ON "Document"("projectId")"#,
            r#"CREATE INDEX "Document_state_idx" ON "Document"(state)"#,
            r#"CREATE INDEX "Document_org_state_idx" ON "Document"("organizationId", state)"#,
            r#"INSERT INTO "Document" (id, "organizationId", "externalId", "createdAt", "deletedAt", "reviewerIds", title)
                VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002',
                            '2025-06-01 12:30:00.123', NULL, '{00000000-0000-0000-0000-000000000009}', 'live'),
                       (2, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002',
                            '2025-06-01 12:30:00.123', '2025-06-02', '{}', 'archived')"#,
            r#"CREATE UNIQUE INDEX "Document_live_external_key" ON "Document"("organizationId", "externalId") WHERE "deletedAt" IS NULL"#,
            r#"CREATE UNIQUE INDEX "Document_live_created_key" ON "Document"("organizationId", "createdAt") WHERE "deletedAt" IS NULL"#,
            r#"CREATE INDEX "Document_org_created_idx" ON "Document"("organizationId", "createdAt")"#,
            r#"COMMIT"#,
        ] {
            run(&mut session, sql);
        }
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut session = admin.sql_session();
        let (_, indexes) = admin.secondary_index_catalog().unwrap();
        assert_eq!(indexes.len(), 11);
        assert_eq!(indexes.iter().filter(|index| index.predicate.is_some()).count(), 2);
        let updated = run(
            &mut session,
            r#"INSERT INTO "Document" (id, "organizationId", "externalId", "createdAt", title)
            VALUES (3, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', '2025-06-03', 'updated')
            ON CONFLICT ("organizationId", "externalId") WHERE "deletedAt" IS NULL
            DO UPDATE SET title = excluded.title RETURNING id, title"#,
        );
        assert_eq!(updated.rows[0].values, [Value::Int(1), text("updated")]);
        let outside = run(
            &mut session,
            r#"INSERT INTO "Document" (id, "organizationId", "externalId", "createdAt", "deletedAt", title)
            VALUES (4, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002',
                       '2025-06-01 12:30:00.123', '2025-06-04', 'another archived')
            ON CONFLICT ("organizationId", "externalId") WHERE "deletedAt" IS NULL
            DO NOTHING RETURNING id, title"#,
        );
        assert_eq!(outside.rows[0].values, [Value::Int(4), text("another archived")]);
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut session = admin.sql_session();
        let rows = run(
            &mut session,
            r#"SELECT id, title FROM "Document" WHERE "createdAt" >= TIMESTAMP '2025-06-01 12:30:00.123' ORDER BY id"#,
        );
        assert_eq!(
            rows.rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
            [
                vec![Value::Int(1), text("updated")],
                vec![Value::Int(2), text("archived")],
                vec![Value::Int(4), text("another archived")],
            ]
        );
        let reviewers = run(
            &mut session,
            r#"SELECT id FROM "Document" WHERE '00000000-0000-0000-0000-000000000009'::uuid = ANY("reviewerIds")"#,
        );
        assert_eq!(reviewers.rows[0].values, [Value::Int(1)]);
    }
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn legacy_wide_document_migration() {
    wide_document_migration(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_wide_document_migration() {
    wide_document_migration(Engine::V2);
}
