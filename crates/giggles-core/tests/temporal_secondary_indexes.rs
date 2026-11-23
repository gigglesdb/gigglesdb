mod common;

use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlResult, StatementClock, Value, take_sql_error_detail, take_sql_error_message},
    sqlstate,
};

fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 0,
        statement_us: 0,
        wall: None,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
    }
}

fn run(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute_at(sql, &[], None, &clock())
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    run(session, sql)
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn error(session: &mut SqlSession, sql: &str, state: &str, message: &str, detail: &str) {
    let error: CoreError = session.execute_at(sql, &[], None, &clock()).unwrap_err();
    assert_eq!(sqlstate(&error), state, "{sql}: {error:?}");
    assert_eq!(take_sql_error_message().as_deref(), Some(message), "{sql}");
    assert_eq!(take_sql_error_detail().as_deref(), Some(detail), "{sql}");
}

fn text(value: &str) -> Value {
    Value::Text(value.into())
}

fn copy(session: &mut SqlSession, statement: &str, values: &[&str]) {
    let (_, copy) = session.prepare_copy(statement).unwrap();
    let clock = clock();
    let (copy, rows) = session
        .normalize_copy_rows_at(
            &copy,
            vec![values.iter().map(|value| text(value)).collect()],
            &clock,
        )
        .unwrap();
    assert_eq!(
        session
            .ingest_copy_at(&copy, rows, &clock)
            .unwrap()
            .affected,
        1
    );
}

fn temporal_indexes(engine: Engine) {
    let path = common::fresh("temporal-secondary-indexes");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut sql = admin.sql_session();
        run(
            &mut sql,
            r#"CREATE TABLE "Obj" (
            id uuid PRIMARY KEY, "organizationId" uuid NOT NULL,
            "createdAt" timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "deletedAt" timestamp(3), tz timestamptz(4) UNIQUE
        )"#,
        );
        run(
            &mut sql,
            r#"INSERT INTO "Obj" (id, "organizationId", "createdAt", tz) VALUES
            ('00000000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000000',
             '2024-01-01 12:30:00.123', '2024-01-01 01:00+01'),
            ('00000000-0000-0000-0000-000000000002', '10000000-0000-0000-0000-000000000000',
             '2024-01-01 12:30:00.1236', 'infinity'),
            ('00000000-0000-0000-0000-000000000003', '10000000-0000-0000-0000-000000000000',
             '-infinity', '-infinity')"#,
        );
        run(
            &mut sql,
            r#"CREATE INDEX "Obj_c_idx" ON "Obj"("createdAt")"#,
        );
        run(
            &mut sql,
            r#"CREATE INDEX "Obj_org_del_idx" ON "Obj"("organizationId", "deletedAt")"#,
        );
        run(
            &mut sql,
            r#"CREATE UNIQUE INDEX "Obj_org_created_key" ON "Obj"("organizationId", "createdAt")"#,
        );
        error(
            &mut sql,
            r#"INSERT INTO "Obj" VALUES ('00000000-0000-0000-0000-000000000004',
                '10000000-0000-0000-0000-000000000000','2024-01-01 12:30:00.1229',NULL,NULL)"#,
            "23505",
            r#"duplicate key value violates unique constraint "Obj_org_created_key""#,
            r#"Key ("organizationId", "createdAt")=(10000000-0000-0000-0000-000000000000, 2024-01-01 12:30:00.123) already exists."#,
        );
        error(
            &mut sql,
            r#"UPDATE "Obj" SET tz='2023-12-31 19:00-05'
               WHERE id='00000000-0000-0000-0000-000000000002'"#,
            "23505",
            r#"duplicate key value violates unique constraint "Obj_tz_key""#,
            "Key (tz)=(2024-01-01 00:00:00+00) already exists.",
        );
        assert_eq!(
            rows(
                &mut sql,
                r#"SELECT "createdAt"::text FROM "Obj" ORDER BY "createdAt""#
            ),
            [
                vec![text("-infinity")],
                vec![text("2024-01-01 12:30:00.123")],
                vec![text("2024-01-01 12:30:00.124")]
            ]
        );
        let insert = r#"INSERT INTO "Obj" (id, "organizationId", "createdAt") VALUES
            ('00000000-0000-0000-0000-000000000004',
             '10000000-0000-0000-0000-000000000000','2024-01-01 12:30:00.123')
            ON CONFLICT ("organizationId", "createdAt")"#;
        assert_eq!(run(&mut sql, &format!("{insert} DO NOTHING")).affected, 0);
        let update = run(
            &mut sql,
            &format!(r#"{insert} DO UPDATE SET "deletedAt" = EXCLUDED."createdAt" RETURNING id"#),
        );
        assert_eq!(update.affected, 1);
        assert_eq!(
            update.rows[0].values,
            [text("00000000-0000-0000-0000-000000000001")]
        );
        run(&mut sql, "BEGIN");
        run(&mut sql, "SAVEPOINT index_changes");
        run(
            &mut sql,
            r#"DELETE FROM "Obj" WHERE "createdAt"='2024-01-01 12:30:00.123'"#,
        );
        assert_eq!(run(&mut sql, &format!("{insert} DO NOTHING")).affected, 1);
        run(&mut sql, "ROLLBACK TO SAVEPOINT index_changes");
        assert_eq!(run(&mut sql, &format!("{insert} DO NOTHING")).affected, 0);
        run(&mut sql, "COMMIT");
        run(
            &mut sql,
            r#"DELETE FROM "Obj" WHERE id='00000000-0000-0000-0000-000000000002'"#,
        );
        copy(
            &mut sql,
            r#"COPY "Obj" (id, "organizationId", "createdAt", tz) FROM STDIN"#,
            &[
                "00000000-0000-0000-0000-000000000005",
                "10000000-0000-0000-0000-000000000000",
                "2024-01-01 12:30:00.124",
                "infinity",
            ],
        );

        run(
            &mut sql,
            "CREATE TABLE temporal_values (id int PRIMARY KEY, d date, t time(3),
            z timetz, span interval, at timestamp(6), CONSTRAINT day_key UNIQUE(d, t))",
        );
        run(
            &mut sql,
            "CREATE UNIQUE INDEX span_key ON temporal_values(span)",
        );
        run(
            &mut sql,
            "CREATE UNIQUE INDEX zoned_time_key ON temporal_values(z)",
        );
        run(
            &mut sql,
            "INSERT INTO temporal_values VALUES
            (1, 'infinity', '24:00', '12:00+02', '1 month', 'infinity'),
            (2, '-infinity', '00:00', '10:00+00', '-infinity', '-infinity'),
            (3, '2024-01-01', '12:00:00.1236', '11:00+02', 'infinity', '2000-01-01')",
        );
        error(
            &mut sql,
            "INSERT INTO temporal_values (id, span) VALUES (0, '30 days')",
            "23505",
            r#"duplicate key value violates unique constraint "span_key""#,
            "Key (span)=(30 days) already exists.",
        );
        error(
            &mut sql,
            "INSERT INTO temporal_values (id, z) VALUES (4, '12:00+02')",
            "23505",
            r#"duplicate key value violates unique constraint "zoned_time_key""#,
            "Key (z)=(12:00:00+02) already exists.",
        );
        error(
            &mut sql,
            "INSERT INTO temporal_values (id, d, t) VALUES (4, '2024-01-01','12:00:00.124')",
            "23505",
            r#"duplicate key value violates unique constraint "day_key""#,
            "Key (d, t)=(2024-01-01, 12:00:00.124) already exists.",
        );
        assert_eq!(
            rows(&mut sql, "SELECT id FROM temporal_values ORDER BY z"),
            [
                vec![Value::Int(3)],
                vec![Value::Int(1)],
                vec![Value::Int(2)]
            ]
        );
        assert_eq!(
            rows(&mut sql, "SELECT id FROM temporal_values ORDER BY span"),
            [
                vec![Value::Int(2)],
                vec![Value::Int(1)],
                vec![Value::Int(3)]
            ]
        );
        run(&mut sql, "CREATE INDEX by_time ON temporal_values(t)");
        run(&mut sql, "CREATE INDEX by_stamp ON temporal_values(at)");
        run(
            &mut sql,
            "CREATE TABLE duplicates (id int PRIMARY KEY, g int, at timestamp)",
        );
        run(
            &mut sql,
            "INSERT INTO duplicates VALUES (1,1,'2024-01-01'),(2,1,'2024-01-01')",
        );
        error(
            &mut sql,
            "CREATE UNIQUE INDEX duplicates_t ON duplicates(at)",
            "23505",
            r#"could not create unique index "duplicates_t""#,
            "Key (at)=(2024-01-01 00:00:00) is duplicated.",
        );
        error(
            &mut sql,
            "CREATE UNIQUE INDEX duplicates_g ON duplicates(g)",
            "23505",
            r#"could not create unique index "duplicates_g""#,
            "Key (g)=(1) is duplicated.",
        );
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut sql = admin.sql_session();
        assert_eq!(
            rows(
                &mut sql,
                r#"SELECT "createdAt"::text FROM "Obj"
            WHERE "createdAt" >= '2024-01-01' AND "createdAt" < '2024-01-02'
            ORDER BY "createdAt" DESC"#
            ),
            [
                vec![text("2024-01-01 12:30:00.124")],
                vec![text("2024-01-01 12:30:00.123")]
            ]
        );
        assert_eq!(
            rows(
                &mut sql,
                r#"SELECT "deletedAt"::text FROM "Obj"
            WHERE tz='2023-12-31 19:00-05'"#
            ),
            [vec![text("2024-01-01 12:30:00.123")]]
        );
        let catalog = admin.catalog_presentation().unwrap();
        let constraint = catalog
            .constraints
            .iter()
            .find(|c| c.name == "Obj_tz_key")
            .unwrap();
        assert_eq!(constraint.kind, 'u');
        assert_eq!(constraint.columns.len(), 1);
        error(
            &mut sql,
            "INSERT INTO temporal_values (id, span) VALUES (8, '30 days')",
            "23505",
            r#"duplicate key value violates unique constraint "span_key""#,
            "Key (span)=(30 days) already exists.",
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

fn column_names(engine: Engine) {
    let path = common::fresh("reserved-column-names");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut sql = admin.sql_session();
        run(
            &mut sql,
            r#"CREATE TABLE "Organization" (id uuid PRIMARY KEY, name text)"#,
        );
        run(
            &mut sql,
            r#"INSERT INTO "Organization" VALUES
            ('00000000-0000-0000-0000-000000000001','o1'),
            ('00000000-0000-0000-0000-000000000002','o2')"#,
        );
        run(
            &mut sql,
            r#"CREATE TABLE "DesignDocSettings" (
            "organizationId" UUID PRIMARY KEY REFERENCES "Organization"(id) ON DELETE CASCADE,
            "approvals" BOOLEAN NOT NULL DEFAULT false,
            sizes BOOLEAN NOT NULL DEFAULT false,
            "evidenceTracking" BOOLEAN NOT NULL DEFAULT false)"#,
        );
        let insert = r#"INSERT INTO "DesignDocSettings" ("organizationId")
            VALUES ('00000000-0000-0000-0000-000000000001')"#;
        run(&mut sql, insert);
        error(
            &mut sql,
            insert,
            "23505",
            r#"duplicate key value violates unique constraint "DesignDocSettings_pkey""#,
            r#"Key ("organizationId")=(00000000-0000-0000-0000-000000000001) already exists."#,
        );
        let rejected = sql
            .execute(
                r#"INSERT INTO "DesignDocSettings" ("organizationId")
            VALUES ('00000000-0000-0000-0000-000000000009')"#,
                &[],
                None,
            )
            .unwrap_err();
        assert_eq!(sqlstate(&rejected), "23503");
        assert_eq!(
            take_sql_error_detail().as_deref(),
            Some(
                r#"Key (organizationId)=(00000000-0000-0000-0000-000000000009) is not present in table "Organization"."#
            )
        );
        for val in ["true", "false"] {
            run(
                &mut sql,
                &format!(
                    r#"INSERT INTO "DesignDocSettings" ("organizationId", approvals)
                VALUES ('00000000-0000-0000-0000-000000000002',{val})
                ON CONFLICT ("organizationId") DO UPDATE SET approvals=EXCLUDED.approvals"#
                ),
            );
        }
        run(
            &mut sql,
            r#"DELETE FROM "Organization" WHERE id='00000000-0000-0000-0000-000000000001'"#,
        );
        run(
            &mut sql,
            "CREATE TABLE named_columns (id int PRIMARY KEY, tasks int, operations text,
            system int, internal int, pg_x int, giggles_y int)",
        );
        run(
            &mut sql,
            "ALTER TABLE named_columns ADD COLUMN receipts int",
        );
        run(
            &mut sql,
            "ALTER TABLE named_columns RENAME COLUMN receipts TO repair_x",
        );
        run(&mut sql, "ALTER TABLE named_columns ADD UNIQUE (tasks)");
        run(
            &mut sql,
            "CREATE INDEX named_cols_idx ON named_columns(pg_x, repair_x)",
        );
        run(
            &mut sql,
            "CREATE TABLE named_child (id int PRIMARY KEY,
            approvals int REFERENCES named_columns(tasks) ON DELETE CASCADE)",
        );
        run(&mut sql, "INSERT INTO named_columns (id,tasks,operations,system,internal,pg_x,giggles_y,repair_x)
            VALUES (1,2,'ok',3,4,5,6,7)");
        copy(
            &mut sql,
            "COPY named_child (id, approvals) FROM STDIN",
            &["3", "2"],
        );
        run(
            &mut sql,
            "UPDATE named_columns SET pg_x = pg_x + 1 WHERE tasks=2",
        );
        for statement in [
            "CREATE TABLE approvals (id int PRIMARY KEY)",
            "CREATE TABLE bad (giggles_rowid bigint PRIMARY KEY)",
            "ALTER TABLE named_columns RENAME COLUMN tasks TO giggles_rowid",
            "CREATE INDEX pg_protected ON named_columns (tasks)",
        ] {
            let err = sql.execute(statement, &[], None).unwrap_err();
            assert_eq!(sqlstate(&err), "0A000", "{statement}: {err:?}");
        }
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut sql = admin.sql_session();
        assert_eq!(
            rows(
                &mut sql,
                r#"SELECT "organizationId"::text, approvals FROM "DesignDocSettings""#
            ),
            [vec![
                text("00000000-0000-0000-0000-000000000002"),
                Value::Bool(false)
            ]]
        );
        let catalog = admin.catalog_presentation().unwrap();
        let constraint = catalog
            .constraints
            .iter()
            .find(|c| c.name == "DesignDocSettings_organizationId_fkey")
            .unwrap();
        assert_eq!(constraint.kind, 'f');
        assert_eq!(constraint.delete, 'c');
        assert_eq!(
            rows(
                &mut sql,
                "SELECT n.tasks,n.operations,n.system,n.internal,n.pg_x,n.giggles_y,n.repair_x FROM named_columns n"
            ),
            [vec![
                Value::Int(2),
                text("ok"),
                Value::Int(3),
                Value::Int(4),
                Value::Int(6),
                Value::Int(6),
                Value::Int(7)
            ]]
        );
        run(&mut sql, "DELETE FROM named_columns WHERE tasks=2");
        assert!(rows(&mut sql, "SELECT approvals FROM named_child").is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn temporal_indexes_legacy() {
    temporal_indexes(Engine::Legacy);
}

#[test]
fn column_names_legacy() {
    column_names(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn temporal_indexes_v2() {
    temporal_indexes(Engine::V2);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn column_names_v2() {
    column_names(Engine::V2);
}

fn temporal_partial_upsert(engine: Engine) {
    let path = common::fresh("partial-temporal-reopen");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for sql in [
            r#"CREATE TABLE am_temporal (
                id int PRIMARY KEY, "organizationId" int, "createdAt" timestamp(3),
                "deletedAt" timestamptz, approvals boolean, label text
            )"#,
            r#"CREATE UNIQUE INDEX am_live ON am_temporal ("organizationId", "createdAt")
                WHERE "deletedAt" IS NULL"#,
            r#"CREATE UNIQUE INDEX am_approved ON am_temporal ("organizationId") WHERE approvals"#,
            r#"INSERT INTO am_temporal VALUES
                (10, 1, '2026-04-15 12:30:00.1234', NULL, false, 'old'),
                (20, 1, '2026-04-15 12:30:00.1234', '2026-04-16 00:00:00+00', true, 'archived')"#,
        ] {
            run(&mut s, sql);
        }
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        for (proposal, expected) in [
            ("(1, 1, '2026-04-15 12:30:00.123', NULL, false, 'new')", (10, "new")),
            (
                "(2, 1, '2026-04-15 12:30:00.123', '2026-04-17 00:00:00+00', false, 'outside')",
                (2, "outside"),
            ),
        ] {
            let result = run(
                &mut s,
                &format!(
                    r#"INSERT INTO am_temporal VALUES {proposal}
                       ON CONFLICT ("organizationId", "createdAt") WHERE "deletedAt" IS NULL
                       DO UPDATE SET label = excluded.label RETURNING id, label"#
                ),
            );
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0].values, [Value::Int(expected.0), text(expected.1)]);
        }
        error(
            &mut s,
            "INSERT INTO am_temporal VALUES (3, 1, '2026-04-15 12:30:00.123', NULL, false, 'duplicate')",
            "23505",
            r#"duplicate key value violates unique constraint "am_live""#,
            r#"Key ("organizationId", "createdAt")=(1, 2026-04-15 12:30:00.123) already exists."#,
        );
        error(
            &mut s,
            "UPDATE am_temporal SET approvals = true WHERE id = 10",
            "23505",
            r#"duplicate key value violates unique constraint "am_approved""#,
            r#"Key ("organizationId")=(1) already exists."#,
        );
        run(&mut s, r#"UPDATE am_temporal SET "deletedAt" = '2026-04-18 00:00:00+00' WHERE id = 10"#);
        run(&mut s, r#"UPDATE am_temporal SET "deletedAt" = NULL WHERE id = 2"#);
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let result = run(
            &mut admin.sql_session(),
            r#"SELECT id, label, "deletedAt" IS NULL AS live FROM am_temporal ORDER BY id"#,
        );
        assert_eq!(
            result.rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
            [(2, "outside", true), (10, "new", false), (20, "archived", false)]
                .map(|(id, label, live)| vec![Value::Int(id), text(label), Value::Bool(live)])
        );
    }
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn legacy_temporal_partial_upsert() {
    temporal_partial_upsert(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_temporal_partial_upsert() {
    temporal_partial_upsert(Engine::V2);
}
