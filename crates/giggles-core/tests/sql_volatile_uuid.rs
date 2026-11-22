mod common;

use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{
        SqlError, SqlResult, StatementClock, Value, take_sql_error_detail, take_sql_error_message,
    },
};
use std::time::{SystemTime, UNIX_EPOCH};

fn run(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn duplicate(session: &mut SqlSession, sql: &str, message: &str, detail: &str) {
    assert_eq!(
        session.execute(sql, &[], None),
        Err(CoreError::Sql(SqlError::Duplicate)),
        "{sql}"
    );
    assert_eq!(take_sql_error_message().as_deref(), Some(message), "{sql}");
    assert_eq!(take_sql_error_detail().as_deref(), Some(detail), "{sql}");
}

fn verify_schema(engine: Engine) {
    let path = common::fresh("volatile-uuid");
    let ordered;
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut sql = admin.sql_session();
        run(
            &mut sql,
            r#"CREATE TABLE "User" (
            "id" UUID NOT NULL DEFAULT pg_catalog.gen_random_uuid(),
            "email" TEXT NOT NULL, "orgId" UUID, "name" TEXT,
            CONSTRAINT "User_pkey" PRIMARY KEY ("id"))"#,
        );
        run(
            &mut sql,
            r#"INSERT INTO "User" ("email") VALUES ('a@x'),('b@x'),('c@x')"#,
        );
        run(
            &mut sql,
            r#"CREATE UNIQUE INDEX "User_email_key" ON "User"("email")"#,
        );
        run(
            &mut sql,
            r#"CREATE INDEX "User_orgId_idx" ON "User"("orgId")"#,
        );
        run(
            &mut sql,
            r#"CREATE UNIQUE INDEX "User_orgId_name_key" ON "User"("orgId", "name")"#,
        );
        duplicate(
            &mut sql,
            r#"INSERT INTO "User" ("email") VALUES ('a@x')"#,
            r#"duplicate key value violates unique constraint "User_email_key""#,
            "Key (email)=(a@x) already exists.",
        );
        run(
            &mut sql,
            "CREATE TABLE input_rows (id int PRIMARY KEY, email text)",
        );
        run(
            &mut sql,
            "INSERT INTO input_rows VALUES (1,'z1'),(2,'z2'),(3,'z3'),(4,'y1'),(5,'y2'),(6,'y3')",
        );
        run(
            &mut sql,
            r#"INSERT INTO "User" ("email") SELECT email FROM input_rows WHERE id <= 3"#,
        );
        run(
            &mut sql,
            r#"INSERT INTO "User" ("id","email") SELECT gen_random_uuid(), email FROM input_rows WHERE id > 3"#,
        );
        run(
            &mut sql,
            r#"UPDATE "User" SET "orgId" = gen_random_uuid() WHERE email LIKE 'z%'"#,
        );
        let orgs = run(
            &mut sql,
            r#"SELECT count(DISTINCT "orgId") FROM "User" WHERE email LIKE 'z%'"#,
        );
        assert_eq!(orgs.rows[0].values, [Value::Int(3)]);
        ordered = run(&mut sql, r#"SELECT id FROM "User" ORDER BY email"#).rows;
        assert_eq!(ordered.len(), 9);
        let distinct = ordered
            .iter()
            .map(|row| {
                let Value::Text(uuid) = &row.values[0] else {
                    panic!("expected UUID");
                };
                assert_eq!(uuid.as_bytes()[14], b'7');
                assert!(b"89ab".contains(&uuid.as_bytes()[19]));
                uuid
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(distinct.len(), 9);

        run(
            &mut sql,
            "CREATE TABLE t2 (id uuid PRIMARY KEY, a uuid, b int)",
        );
        run(
            &mut sql,
            "ALTER TABLE t2 ADD CONSTRAINT t2_a_b_key UNIQUE (a, b)",
        );
        run(
            &mut sql,
            "INSERT INTO t2 VALUES
            ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002',1),
            ('00000000-0000-0000-0000-000000000005',NULL,1),
            ('00000000-0000-0000-0000-000000000006',NULL,1)",
        );
        duplicate(
            &mut sql,
            "INSERT INTO t2 VALUES ('00000000-0000-0000-0000-000000000001',NULL,2)",
            r#"duplicate key value violates unique constraint "t2_pkey""#,
            "Key (id)=(00000000-0000-0000-0000-000000000001) already exists.",
        );
        duplicate(
            &mut sql,
            "UPDATE t2 SET a='00000000-0000-0000-0000-000000000002' WHERE id='00000000-0000-0000-0000-000000000005'",
            r#"duplicate key value violates unique constraint "t2_a_b_key""#,
            "Key (a, b)=(00000000-0000-0000-0000-000000000002, 1) already exists.",
        );
        duplicate(
            &mut sql,
            "CREATE UNIQUE INDEX t2_dup ON t2 (b)",
            r#"could not create unique index "t2_dup""#,
            "Key (b)=(1) is duplicated.",
        );
        run(&mut sql, r#"DROP INDEX "User_orgId_idx""#);
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut sql = admin.sql_session();
        assert_eq!(
            run(&mut sql, r#"SELECT id FROM "User" ORDER BY email"#).rows,
            ordered
        );
        duplicate(
            &mut sql,
            r#"INSERT INTO "User" ("email") VALUES ('a@x')"#,
            r#"duplicate key value violates unique constraint "User_email_key""#,
            "Key (email)=(a@x) already exists.",
        );
        run(
            &mut sql,
            r#"INSERT INTO "User" ("email","orgId","name")
            VALUES ('r@x','00000000-0000-0000-0000-00000000000a','n')"#,
        );
        duplicate(
            &mut sql,
            r#"INSERT INTO "User" ("email","orgId","name") VALUES ('s@x','00000000-0000-0000-0000-00000000000A','n')"#,
            r#"duplicate key value violates unique constraint "User_orgId_name_key""#,
            r#"Key ("orgId", name)=(00000000-0000-0000-0000-00000000000a, n) already exists."#,
        );
        let row = run(
            &mut sql,
            r#"SELECT uuid_extract_version(id) FROM "User" WHERE email = 'r@x'"#,
        );
        assert_eq!(row.rows[0].values, [Value::Int(7)]);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn prisma_indexes_and_generated_rows_survive_legacy_restart() {
    verify_schema(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn prisma_indexes_and_generated_rows_survive_v2_restart() {
    verify_schema(Engine::V2);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn ten_thousand_defaults_are_strictly_increasing() {
    let path = common::fresh("volatile-uuid-order");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut sql = admin.sql_session();
        run(
            &mut sql,
            "CREATE TABLE large (n int PRIMARY KEY, u uuid DEFAULT gen_random_uuid())",
        );
        run(&mut sql, "CREATE TABLE input_rows (n int PRIMARY KEY)");
        for start in (1..=10_000).step_by(200) {
            let values = (start..start + 200)
                .map(|n| format!("({n})"))
                .collect::<Vec<_>>()
                .join(",");
            run(&mut sql, &format!("INSERT INTO input_rows VALUES {values}"));
        }
        run(
            &mut sql,
            "INSERT INTO large(n) SELECT n FROM input_rows ORDER BY n",
        );
        let prepared = sql.prepare("SELECT n,u FROM large ORDER BY n").unwrap();
        let mut cursor = sql.open_cursor(&prepared, &[]).unwrap();
        let mut last = String::new();
        let mut n = 0;
        while !cursor.is_exhausted() {
            for row in cursor.fetch(500, 1024 * 1024).unwrap() {
                let [Value::Int(sequence), Value::Text(uuid)] = row.values.as_slice() else {
                    panic!("expected ordered UUID row");
                };
                n += 1;
                assert_eq!(*sequence, n);
                assert!(&last < uuid, "{n}: {last} >= {uuid}");
                last.clone_from(uuid);
                assert_eq!(uuid.as_bytes()[14], b'7');
                assert!(b"89ab".contains(&uuid.as_bytes()[19]));
            }
        }
        assert_eq!(n, 10_000);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn uuid_functions_use_pg_layout_and_timestamp_extraction() {
    let path = common::fresh("volatile-uuid-functions");
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut sql = admin.sql_session();
        let instant = || {
            (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64)
                - 946_684_800_000_000
        };
        let before = instant();
        let wall = std::sync::Arc::new(move || Ok(instant()));
        let clock = StatementClock {
            unavailable: None,
            transaction_us: before,
            statement_us: before,
            wall: Some(wall),
            zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        };
        let result = sql
            .execute_at(
                "SELECT uuid_extract_version(pg_catalog.gen_random_uuid()),
            uuid_extract_version(uuidv4()), uuid_extract_version(uuidv7(interval '-1 day')),
            uuid_extract_timestamp(uuidv4()) IS NULL, uuidv7(NULL) IS NULL,
            uuid_extract_timestamp(gen_random_uuid()) BETWEEN now() - interval '1 second'
                AND clock_timestamp() + interval '1 second'",
                &[],
                None,
                &clock,
            )
            .unwrap();
        assert_eq!(
            result.rows[0].values,
            [
                Value::Int(7),
                Value::Int(4),
                Value::Int(7),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
            ]
        );
        let v7 = run(
            &mut sql,
            "SELECT gen_random_uuid(), uuidv7(), gen_random_uuid()",
        );
        assert!(
            v7.rows[0]
                .values
                .windows(2)
                .all(|v| { matches!(v, [Value::Text(a), Value::Text(b)] if a < b) })
        );
        let row = run(
            &mut sql,
            "SELECT uuid_extract_timestamp('019535d9-3df7-79fb-b466-fa907fa17f9e')",
        );
        let Value::TimestampTz(timestamp) = row.rows[0].values[0] else {
            panic!("expected timestamptz");
        };
        assert_eq!(timestamp.microseconds(), 793_680_384_503_000);
        for shift in ["1000000 years", "-100000 years"] {
            let result = sql.execute(&format!("SELECT uuidv7(interval '{shift}')"), &[], None);
            assert_eq!(result, Err(CoreError::Sql(SqlError::DatetimeFieldOverflow)));
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn random_defaults_and_seed_are_session_scoped() {
    let path = common::fresh("volatile-random");
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut a = admin.sql_session();
        let mut b = admin.sql_session();
        assert_eq!(
            run(&mut a, "SELECT setseed(0.5) IS NULL").rows[0].values,
            [Value::Bool(false)]
        );
        run(&mut b, "SELECT setseed(-0.5)");
        let expect = Value::Float8(giggles_pg_types::Float8::from_bits(
            0.9851677175347999f64.to_bits(),
        ));
        assert_ne!(
            run(&mut b, "SELECT random()").rows[0].values.as_slice(),
            std::slice::from_ref(&expect)
        );
        assert_eq!(run(&mut a, "SELECT random()").rows[0].values, [expect]);
        assert_eq!(
            run(&mut a, "SELECT pg_typeof(setseed(0.5))").rows[0].values,
            [Value::Text("void".into())]
        );
        let result = run(
            &mut a,
            "SELECT random(1.5,2.25), pg_typeof(random(1,2)), pg_typeof(random(1::bigint,2))",
        );
        assert_eq!(
            result.rows[0].values,
            [
                Value::Numeric(giggles_pg_types::Numeric::parse("1.66").unwrap()),
                Value::Text("integer".into()),
                Value::Text("bigint".into()),
            ]
        );
        run(&mut a, "SELECT setseed(0.5)");
        let range = run(
            &mut a,
            "SELECT random(-123456789012345678901234567890.1234,223456789012345678901234567890.12345)::text",
        );
        assert_eq!(
            range.rows[0].values,
            [Value::Text("223168286424148291625695224447.14452".into())]
        );

        run(
            &mut a,
            "CREATE TABLE defaults (id int PRIMARY KEY, r float8 DEFAULT pg_catalog.random(),
            v uuid DEFAULT uuidv4(), w uuid DEFAULT uuidv7())",
        );
        run(&mut a, "INSERT INTO defaults(id) VALUES (1),(2),(3)");
        let (_, copy) = a.prepare_copy("COPY defaults (id) FROM STDIN").unwrap();
        let clock = StatementClock {
            unavailable: None,
            transaction_us: 0,
            statement_us: 0,
            wall: None,
            zone: None,
        };
        let (copy, rows) = a
            .normalize_copy_rows_at(
                &copy,
                vec![vec![Value::Text("4".into())], vec![Value::Text("5".into())]],
                &clock,
            )
            .unwrap();
        a.ingest_copy_at(&copy, rows, &clock).unwrap();
        assert_eq!(
            run(
                &mut a,
                "SELECT count(DISTINCT r), count(DISTINCT v), count(DISTINCT w)
            FROM defaults WHERE random() < 1"
            )
            .rows[0]
                .values,
            [Value::Int(5), Value::Int(5), Value::Int(5)]
        );
        assert_eq!(
            a.execute("SELECT random(5,2)", &[], None),
            Err(CoreError::Sql(SqlError::InvalidParameterValue))
        );
        assert_eq!(
            take_sql_error_message().as_deref(),
            Some("lower bound must be less than or equal to upper bound")
        );
        assert_eq!(
            a.execute("SELECT setseed(2)", &[], None),
            Err(CoreError::Sql(SqlError::InvalidParameterValue))
        );
        assert_eq!(
            take_sql_error_message().as_deref(),
            Some("setseed parameter 2 is out of allowed range [-1,1]")
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}


#[test]
fn argument_defaults_bind_at_ddl_and_run_on_each_insert() {
    let path = common::fresh("volatile-uuid-argument-defaults");
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut sql = admin.sql_session();
        run(
            &mut sql,
            "CREATE TABLE d (id int PRIMARY KEY, r int DEFAULT random(1,2),
             n numeric DEFAULT random(1.5, 2.25), u uuid DEFAULT uuidv7(interval '1 day'),
             v smallint DEFAULT uuid_extract_version(gen_random_uuid()),
             ts timestamptz DEFAULT uuid_extract_timestamp(uuidv7()))",
        );
        for (ddl, expected) in [
            ("ALTER TABLE d ALTER COLUMN r SET DEFAULT random(1)", SqlError::UndefinedFunction),
            ("CREATE TABLE bad (id uuid DEFAULT uuidv7(1))", SqlError::UndefinedFunction),
            ("CREATE TABLE bad (id int DEFAULT random(1+1,2))", SqlError::Unsupported),
        ] {
            assert_eq!(sql.execute(ddl, &[], None), Err(CoreError::Sql(expected)), "{ddl}");
        }
        for (ddl, message) in [
            (r#"CREATE TABLE bad (id uuid DEFAULT "pg_catalog.uuidv7"())"#,
             "function pg_catalog.uuidv7() does not exist"),
            (r#"ALTER TABLE d ALTER COLUMN r SET DEFAULT "pg_catalog.random"(1,2)"#,
             "function pg_catalog.random(integer, integer) does not exist"),
        ] {
            assert_eq!(sql.execute(ddl, &[], None), Err(CoreError::Sql(SqlError::UndefinedFunction)));
            assert_eq!(take_sql_error_message().as_deref(), Some(message));
            assert_eq!(
                giggles_core::relational::take_sql_error_hint(),
                Some("No function matches the given name and argument types. You might need to add explicit type casts."),
            );
        }
        run(&mut sql, "CREATE TABLE qualified (id uuid DEFAULT pg_catalog.uuidv7())");
        run(&mut sql, "INSERT INTO qualified VALUES (DEFAULT)");
        assert_eq!(
            run(&mut sql, "SELECT uuid_extract_version(id) FROM qualified").rows[0].values,
            [Value::Int(7)],
        );
        run(&mut sql, "INSERT INTO d (id) VALUES (1),(2)");
        run(&mut sql, "INSERT INTO d VALUES (3,DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT)");
        let prepared = sql.prepare("INSERT INTO d (id) VALUES ($1)").unwrap();
        sql.execute_prepared(&prepared, &[Value::Int(4)], None).unwrap();

        run(&mut sql, "ALTER TABLE d ALTER COLUMN r SET DEFAULT random(5,6)");
        let explicit = sql.prepare("INSERT INTO d VALUES ($1,DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT)").unwrap();
        for id in [5, 6] {
            sql.execute_prepared(&explicit, &[Value::Int(id)], None).unwrap();
        }
        let rows = run(
            &mut sql,
            "SELECT r BETWEEN 1 AND 2, r BETWEEN 5 AND 6, n BETWEEN 1.5 AND 2.25,
             uuid_extract_version(u), v, ts IS NOT NULL
             FROM d ORDER BY id",
        ).rows;
        assert_eq!(rows.len(), 6);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(
                row.values,
                [Value::Bool(i < 4), Value::Bool(i >= 4), Value::Bool(true),
                 Value::Int(7), Value::Int(7), Value::Bool(true)],
            );
        }
        let distinct = run(&mut sql, "SELECT count(DISTINCT u) FROM d");
        assert_eq!(distinct.rows[0].values, [Value::Int(6)]);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn latest_collation_collision_reports_the_proposed_email() {
    let path = common::fresh("volatile-uuid-latest-collation");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut current = admin.sql_session();
        run(&mut current, "CREATE COLLATION ci (provider=icu,locale='und-u-ks-level2',deterministic=false)");
        run(&mut current, "CREATE TABLE t (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), email text COLLATE ci UNIQUE)");
        let mut old = admin.sql_session();
        run(&mut old, "BEGIN");
        old.set_transaction_isolation(giggles_core::SqlIsolation::RepeatableRead).unwrap();
        assert_eq!(run(&mut old, "SELECT count(*) FROM t").rows[0].values, [Value::Int(0)]);
        run(&mut current, "INSERT INTO t(email) VALUES ('a@x')");
        duplicate(
            &mut old,
            "INSERT INTO t(email) VALUES ('A@X')",
            r#"duplicate key value violates unique constraint "t_email_key""#,
            "Key (email)=(A@X) already exists.",
        );
        run(&mut old, "ROLLBACK");
    }
    std::fs::remove_dir_all(path).unwrap();
}
