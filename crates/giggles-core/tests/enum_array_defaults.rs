mod common;

use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError, StatementClock, Value, take_sql_error_hint, take_sql_error_message},
};
use giggles_pg_types::temporal::SessionZone;

fn run(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?} ({:?})", take_sql_error_message()))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn text(value: &str) -> Value {
    Value::Text(value.into())
}

fn error(session: &mut SqlSession, sql: &str, expected: SqlError, message: &str) {
    assert_eq!(
        session.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
    assert_eq!(take_sql_error_message().as_deref(), Some(message), "{sql}");
}

#[test]
fn quoted_enum_columns_and_atomic_defaults() {
    let db = Admin::create(common::fresh("quoted-enum-columns"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    for statement in [
        r#"CREATE TYPE "Src" AS ENUM ('MANUAL','IMPORT')"#,
        "CREATE TYPE k_lower AS ENUM ('A','B')",
        r#"CREATE TABLE "Obj" (id integer PRIMARY KEY, metadata JSONB, type TEXT, keep TEXT NOT NULL)"#,
        r#"INSERT INTO "Obj" VALUES (1,'{}','a','k1'),(2,NULL,'b','k2')"#,
        r#"ALTER TABLE "Obj" DROP COLUMN metadata, DROP COLUMN type, ADD COLUMN "kindId" INTEGER, ADD COLUMN source public."Src" NOT NULL DEFAULT 'MANUAL', ALTER COLUMN keep DROP NOT NULL"#,
        r#"CREATE TABLE tokens (id int PRIMARY KEY, status "k_lower" DEFAULT 'A', draft k_lower NOT NULL DEFAULT 'B', source "Src" DEFAULT 'IMPORT')"#,
        r#"ALTER TABLE tokens ADD COLUMN draft2 "k_lower" DEFAULT 'A'"#,
        "INSERT INTO tokens (id) VALUES (1)",
        r#"ALTER TABLE "Obj" ADD COLUMN old_source TEXT"#,
        r#"UPDATE "Obj" SET old_source = 'IMPORT' WHERE id = 1"#,
    ] {
        run(&mut sql, statement);
    }
    assert_eq!(
        run(&mut sql, r#"SELECT * FROM "Obj" ORDER BY id"#),
        vec![
            vec![
                Value::Int(1),
                text("k1"),
                Value::Null,
                text("MANUAL"),
                text("IMPORT")
            ],
            vec![
                Value::Int(2),
                text("k2"),
                Value::Null,
                text("MANUAL"),
                Value::Null
            ],
        ],
    );
    error(
        &mut sql,
        r#"ALTER TABLE "Obj" ADD COLUMN z1 TEXT, ADD COLUMN z2 INTEGER NOT NULL"#,
        SqlError::Null,
        r#"column "z2" of relation "Obj" contains null values"#,
    );
    error(
        &mut sql,
        r#"ALTER TABLE "Obj" ADD COLUMN z1 TEXT, DROP COLUMN nope"#,
        SqlError::UnknownColumn,
        r#"column "nope" of relation "Obj" does not exist"#,
    );
    error(
        &mut sql,
        r#"ALTER TABLE "Obj" ADD COLUMN z1 TEXT, ADD COLUMN z1 INTEGER"#,
        SqlError::DuplicateColumn,
        r#"column "z1" of relation "Obj" already exists"#,
    );
    error(
        &mut sql,
        r#"ALTER TABLE "Obj" ADD COLUMN z1 TEXT, ADD COLUMN bad "Src" NOT NULL DEFAULT 'NOPE'"#,
        SqlError::InvalidTextRepresentation,
        r#"invalid input value for enum "Src": "NOPE""#,
    );
    error(
        &mut sql,
        r#"CREATE TABLE bad_default (id int PRIMARY KEY, source "Src" DEFAULT 'NOPE')"#,
        SqlError::InvalidTextRepresentation,
        r#"invalid input value for enum "Src": "NOPE""#,
    );
    assert_eq!(
        db.catalog()
            .unwrap()
            .1
            .iter()
            .find(|t| t.name == "Obj")
            .unwrap()
            .columns
            .len(),
        5
    );

    error(
        &mut sql,
        r#"ALTER TABLE "Obj" ALTER COLUMN old_source TYPE "Src""#,
        SqlError::Type,
        r#"column "old_source" cannot be cast automatically to type "Src""#,
    );
    run(
        &mut sql,
        r#"ALTER TABLE "Obj" ALTER COLUMN old_source TYPE "Src" USING CAST(old_source AS "Src")"#,
    );
    run(
        &mut sql,
        r#"ALTER TABLE "Obj" ALTER COLUMN old_source SET DATA TYPE public."Src""#,
    );
    assert_eq!(
        run(
            &mut sql,
            r#"SELECT old_source, pg_typeof(old_source) FROM "Obj" ORDER BY id"#
        ),
        vec![
            vec![text("IMPORT"), text(r#""Src""#)],
            vec![Value::Null, text(r#""Src""#)]
        ],
    );
    assert_eq!(
        run(
            &mut sql,
            r#"SELECT 'A'::"k_lower", CAST('MANUAL' AS public."Src"), 'IMPORT'::"Src""#
        ),
        vec![vec![text("A"), text("MANUAL"), text("IMPORT")]],
    );
    assert_eq!(
        run(&mut sql, "SELECT status, draft, source, draft2 FROM tokens"),
        vec![vec![text("A"), text("B"), text("IMPORT"), text("A")]],
    );
    defaults(&db, "Obj", &[("source", r#"'MANUAL'::"Src""#)]);
    defaults(
        &db,
        "tokens",
        &[("status", "'A'::k_lower"), ("source", r#"'IMPORT'::"Src""#)],
    );
}

fn defaults(db: &Admin, table: &str, expected: &[(&str, &str)]) {
    let table = db
        .catalog()
        .unwrap()
        .1
        .into_iter()
        .find(|t| t.name == table)
        .unwrap();
    let defaults = db.catalog_presentation().unwrap().defaults;
    for (column, sql) in expected {
        let column = table.columns.iter().find(|c| c.name == *column).unwrap();
        assert_eq!(
            defaults.get(&column.id).map(String::as_str),
            Some(*sql),
            "{}",
            column.name
        );
    }
}

#[test]
#[cfg_attr(not(feature = "engine-rocksdb"), allow(clippy::single_element_loop))]
fn builtin_defaults_keep_catalog_text_and_backfill() {
    for engine in [
        Engine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        Engine::V2,
    ] {
        let db = Admin::create_engine(common::fresh("builtin-defaults"), Limits::default(), engine)
            .unwrap();
        let mut sql = db.sql_session();
        let clock = StatementClock {
            unavailable: None,
            transaction_us: 1_000_000,
            statement_us: 1_000_000,
            zone: Some(SessionZone::Fixed(0)),
            wall: None,
        };
        run(&mut sql, "CREATE SEQUENCE default_ids");
        for (ty, create, add, set, stored, checks) in [
            ("int", "7", "'8'::integer", "9", ["7", "8", "9"], ["7", "8", "9", "8"]),
            ("bigint", "2147483648", "'2147483649'::bigint", "2147483650",
                ["'2147483648'::bigint", "'2147483649'::bigint", "'2147483650'::bigint"],
                ["2147483648", "2147483649", "2147483650", "2147483649"]),
            ("numeric(7,2)", "1.234", "'2.345'::numeric", "3.456",
                ["1.234", "'2.345'::numeric", "3.456"], ["1.23", "2.35", "3.46", "2.35"]),
            ("boolean", "true", "'false'::boolean", "false",
                ["true", "false", "false"], ["true", "false", "false", "false"]),
            ("text", "'old'", "'add'::text", "'new'",
                ["'old'::text", "'add'::text", "'new'::text"], ["'old'", "'add'", "'new'", "'add'"]),
            ("varchar(5)", "'old'", "'add'", "'new'",
                ["'old'::text", "'add'::text", "'new'::text"], ["'old'", "'add'", "'new'", "'add'"]),
            ("varchar", "'old'", "'add'", "'new'",
                ["'old'::text", "'add'::text", "'new'::text"], ["'old'", "'add'", "'new'", "'add'"]),
            ("character varying(5)", "'old'::character varying", "'add'::varchar", "'new'::character varying",
                ["'old'::character varying", "'add'::character varying", "'new'::character varying"],
                ["'old'", "'add'", "'new'", "'add'"]),
            ("char(3)", "'x'", "'y'", "'z'",
                ["'x'::text", "'y'::text", "'z'::text"], ["'x  '", "'y  '", "'z  '", "'y  '"]),
            ("date", "'2024-01-02'", "'2024-02-03'::date", "'2024-03-04'",
                ["'2024-01-02'::date", "'2024-02-03'::date", "'2024-03-04'::date"],
                ["'2024-01-02'::date", "'2024-02-03'::date", "'2024-03-04'::date", "'2024-02-03'::date"]),
            ("timestamp(3)", "'2024-01-02 03:04:05.1234'", "'2024-02-03 04:05:06.5678'::timestamp", "CURRENT_TIMESTAMP",
                ["'2024-01-02 03:04:05.1234'::timestamp without time zone",
                 "'2024-02-03 04:05:06.5678'::timestamp without time zone", "CURRENT_TIMESTAMP"],
                ["'2024-01-02 03:04:05.123'::timestamp", "'2024-02-03 04:05:06.568'::timestamp",
                 "", "'2024-02-03 04:05:06.568'::timestamp"]),
            ("timestamptz", "now()", "'2024-02-03 04:05:06+00'::timestamptz", "CURRENT_TIMESTAMP",
                ["now()", "'2024-02-03 04:05:06+00'::timestamp with time zone", "CURRENT_TIMESTAMP"],
                ["", "'2024-02-03 04:05:06+00'::timestamptz", "", "'2024-02-03 04:05:06+00'::timestamptz"]),
            ("uuid", "'00000000-0000-0000-0000-000000000001'", "'00000000-0000-0000-0000-000000000002'::uuid", "gen_random_uuid()",
                ["'00000000-0000-0000-0000-000000000001'::text",
                 "'00000000-0000-0000-0000-000000000002'::text", "gen_random_uuid()"],
                ["'00000000-0000-0000-0000-000000000001'",
                 "'00000000-0000-0000-0000-000000000002'", "", "'00000000-0000-0000-0000-000000000002'"]),
            ("jsonb", r#"'{"a":1}'"#, r#"'{"b":2}'::jsonb"#, r#"'{"c":3}'"#,
                [r#"'{"a": 1}'::jsonb"#, r#"'{"b": 2}'::jsonb"#, r#"'{"c": 3}'::jsonb"#],
                [r#"'{"a": 1}'"#, r#"'{"b": 2}'"#, r#"'{"c": 3}'"#, r#"'{"b": 2}'"#]),
            ("bytea", r"'\x61'", r"'\x62'::bytea", r"'\x63'",
                [r"'\x61'::bytea", r"'\x62'::bytea", r"'\x63'::bytea"],
                [r"'\x61'::bytea", r"'\x62'::bytea", r"'\x63'::bytea", r"'\x62'::bytea"]),
            ("bigint", "nextval('default_ids'::regclass)", "nextval('default_ids')", "nextval('default_ids'::regclass)",
                ["nextval('default_ids'::regclass)"; 3], ["1", "2", "3", "4"]),
        ] {
            let [initial, added, updated] = stored;
            let mut execute = |statement: &str| {
                sql.execute_at(statement, &[], None, &clock)
                    .unwrap_or_else(|error| panic!("{engine:?} {ty} {create} / {statement}: {error:?} ({:?})", take_sql_error_message()));
            };
            execute(&format!("CREATE TABLE default_sweep (id int PRIMARY KEY, c {ty} DEFAULT {create})"));
            defaults(&db, "default_sweep", &[("c", initial)]);
            execute("INSERT INTO default_sweep(id) VALUES (1)");
            execute(&format!("ALTER TABLE default_sweep ADD COLUMN a {ty} DEFAULT {add}"));
            defaults(&db, "default_sweep", &[("c", initial), ("a", added)]);
            execute(&format!("ALTER TABLE default_sweep ALTER COLUMN c SET DEFAULT {set}"));
            defaults(&db, "default_sweep", &[("c", updated), ("a", added)]);
            execute("INSERT INTO default_sweep(id) VALUES (2)");
            let rows = run(&mut sql, "SELECT id,c,a FROM default_sweep ORDER BY id");
            assert_eq!(rows.len(), 2);
            assert_eq!((&rows[0][0], &rows[1][0]), (&Value::Int(1), &Value::Int(2)));
            let values = [&rows[0][1], &rows[0][2], &rows[1][1], &rows[1][2]];
            for (value, expected_sql) in values.into_iter().zip(checks) {
                if expected_sql.is_empty() {
                    // The generated value depends on the clock or random source.
                    assert_ne!(*value, Value::Null, "{engine:?} {ty} {create}");
                } else {
                    let expected = run(&mut sql, &format!("SELECT {expected_sql}"));
                    assert_eq!(value, &expected[0][0], "{engine:?} {ty} {create}");
                }
            }
            run(&mut sql, "DROP TABLE default_sweep");
        }
    }
}

#[test]
#[cfg_attr(not(feature = "engine-rocksdb"), allow(clippy::single_element_loop))]
fn enum_array_literals_and_uuid_array_null_defaults() {
    for engine in [
        Engine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        Engine::V2,
    ] {
        let db =
            Admin::create_engine(common::fresh("array-literal-defaults"), Limits::default(), engine)
                .unwrap();
        let mut sql = db.sql_session();
        for statement in [
            r#"CREATE TYPE "Role Kind" AS ENUM ('admin','owner','two words','a,b','NULL')"#,
            r#"CREATE TABLE array_literals (id int PRIMARY KEY, r "Role Kind"[] DEFAULT '{owner}',
                bare uuid[] DEFAULT NULL, typed uuid[] DEFAULT NULL::uuid[], prior uuid[] DEFAULT '{}')"#,
            "INSERT INTO array_literals(id) VALUES (1)",
            r#"ALTER TABLE array_literals ADD COLUMN roles "Role Kind"[] DEFAULT '{"two words","a,b",NULL,"NULL"}'"#,
            "ALTER TABLE array_literals ADD COLUMN added uuid[] DEFAULT NULL::uuid[]",
            "ALTER TABLE array_literals ALTER COLUMN prior SET DEFAULT NULL::uuid[]",
            "ALTER TABLE array_literals ALTER COLUMN r SET DEFAULT '{admin,owner}'",
        ] {
            run(&mut sql, statement);
        }
        for statement in [
            r#"CREATE TABLE bad_enum_literal (id int PRIMARY KEY, r "Role Kind"[] DEFAULT '{owner,missing}')"#,
            "ALTER TABLE array_literals ALTER COLUMN r SET DEFAULT '{admin,missing}'",
            r#"ALTER TABLE array_literals ADD COLUMN bad "Role Kind"[] DEFAULT '{missing}'"#,
        ] {
            error(
                &mut sql,
                statement,
                SqlError::InvalidTextRepresentation,
                r#"invalid input value for enum "Role Kind": "missing""#,
            );
        }
        run(&mut sql, "INSERT INTO array_literals(id) VALUES (2)");
        let roles = text(r#"{"two words","a,b",NULL,"NULL"}"#);
        assert_eq!(
            run(&mut sql, "SELECT id,r,roles,bare,typed,prior,added FROM array_literals ORDER BY id"),
            [
                vec![Value::Int(1), text("{owner}"), roles.clone(), Value::Null, Value::Null, text("{}"), Value::Null],
                vec![Value::Int(2), text("{admin,owner}"), roles, Value::Null, Value::Null, Value::Null, Value::Null],
            ]
        );
        defaults(&db, "array_literals", &[
            ("r", r#"'{admin,owner}'::"Role Kind"[]"#),
            ("roles", r#"'{"two words","a,b",NULL,"NULL"}'::"Role Kind"[]"#),
        ]);
        assert_eq!(db.catalog_presentation().unwrap().defaults.len(), 2);
    }
}

#[test]
fn enum_array_and_jsonb_defaults_preserve_existing_rows() {
    let db = Admin::create(common::fresh("enum-array-defaults"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    for statement in [
        r#"CREATE TYPE "Size" AS ENUM ('SMALL','LARGE')"#,
        "CREATE TYPE lk AS ENUM ('A','B')",
        r#"CREATE TABLE "Tok" ("id" INTEGER NOT NULL, "scopes" TEXT[] DEFAULT ARRAY[]::TEXT[], "grants" TEXT[] DEFAULT ARRAY['authorization_code', 'refresh_token']::TEXT[], "redirectUris" TEXT[], "req" "Size"[] DEFAULT ARRAY[]::"Size"[], "req2" "Size"[] NOT NULL DEFAULT ARRAY['SMALL']::"Size"[], CONSTRAINT "Tok_pkey" PRIMARY KEY ("id"))"#,
        "CREATE TABLE letters (id int PRIMARY KEY, ks lk[])",
        "INSERT INTO letters VALUES (1, '{A,B,NULL}')",
        r#"INSERT INTO "Tok" ("id") VALUES (1)"#,
        r#"INSERT INTO "Tok" ("id","scopes","redirectUris","req") VALUES (2, ARRAY['a','b'], '{x,y}', ARRAY['LARGE','SMALL']::"Size"[])"#,
    ] {
        run(&mut sql, statement);
    }
    assert_eq!(
        run(&mut sql, r#"SELECT * FROM "Tok" ORDER BY id"#),
        vec![
            vec![
                Value::Int(1),
                text("{}"),
                text("{authorization_code,refresh_token}"),
                Value::Null,
                text("{}"),
                text("{SMALL}")
            ],
            vec![
                Value::Int(2),
                text("{a,b}"),
                text("{authorization_code,refresh_token}"),
                text("{x,y}"),
                text("{LARGE,SMALL}"),
                text("{SMALL}")
            ],
        ],
    );
    assert_eq!(
        run(
            &mut sql,
            "SELECT ks, ARRAY[]::lk[], ARRAY['A']::lk[] FROM letters"
        ),
        vec![vec![text("{A,B,NULL}"), text("{}"), text("{A}")]]
    );

    for statement in [
        r#"ALTER TABLE "Tok" ADD COLUMN ex TEXT[] DEFAULT ARRAY[]::TEXT[], ADD COLUMN ex2 TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]"#,
        r#"ALTER TABLE "Tok" ADD COLUMN appr "Size"[]"#,
        r#"ALTER TABLE "Tok" ADD COLUMN req3 "Size"[] NOT NULL DEFAULT ARRAY['LARGE']::"Size"[]"#,
        r#"ALTER TABLE "Tok" ALTER COLUMN appr SET DEFAULT ARRAY[]::"Size"[]"#,
        r#"ALTER TABLE "Tok" ADD COLUMN arr3 TEXT[] DEFAULT '{}'"#,
        r#"ALTER TABLE "Tok" ADD COLUMN j JSONB NOT NULL DEFAULT '{"SMALL":0}', ADD COLUMN j2 JSONB DEFAULT '[]'"#,
        r#"INSERT INTO "Tok" (id) VALUES (3)"#,
        r#"UPDATE "Tok" SET "req" = ARRAY[]::"Size"[] WHERE id = 2"#,
        r#"UPDATE "Tok" SET appr = '{LARGE}' WHERE id = 1"#,
    ] {
        run(&mut sql, statement);
    }
    let expected = vec![
        vec![
            Value::Int(1),
            text("{}"),
            text("{LARGE}"),
            text("{LARGE}"),
            text("{}"),
            text("{}"),
            text(r#"{"SMALL": 0}"#),
            text("[]"),
        ],
        vec![
            Value::Int(2),
            text("{}"),
            Value::Null,
            text("{LARGE}"),
            text("{}"),
            text("{}"),
            text(r#"{"SMALL": 0}"#),
            text("[]"),
        ],
        vec![
            Value::Int(3),
            text("{}"),
            text("{}"),
            text("{LARGE}"),
            text("{}"),
            text("{}"),
            text(r#"{"SMALL": 0}"#),
            text("[]"),
        ],
    ];
    let select = r#"SELECT id, req, appr, req3, ex2, arr3, j, j2 FROM "Tok" ORDER BY id"#;
    assert_eq!(run(&mut sql, select), expected);
    for statement in [
        r#"UPDATE "Tok" SET req = ARRAY['NOPE']::"Size"[] WHERE id = 2"#,
        r#"UPDATE "Tok" SET req = '{NOPE}' WHERE id = 2"#,
        r#"INSERT INTO "Tok" (id, req) VALUES (4, '{NOPE}')"#,
        r#"ALTER TABLE "Tok" ALTER COLUMN req SET DEFAULT ARRAY['NOPE']::"Size"[]"#,
        r#"SELECT ARRAY['NOPE','SMALL']::"Size"[]"#,
    ] {
        error(
            &mut sql,
            statement,
            SqlError::InvalidTextRepresentation,
            r#"invalid input value for enum "Size": "NOPE""#,
        );
    }
    assert_eq!(run(&mut sql, select), expected);
    assert_eq!(
        run(
            &mut sql,
            r#"SELECT ARRAY['SMALL','LARGE']::"Size"[], '{}'::"Size"[], pg_typeof(ARRAY[]::"Size"[])"#
        ),
        vec![vec![text("{SMALL,LARGE}"), text("{}"), text(r#""Size"[]"#)]],
    );
    defaults(
        &db,
        "Tok",
        &[
            ("scopes", "ARRAY[]::text[]"),
            (
                "grants",
                "ARRAY['authorization_code'::text, 'refresh_token'::text]",
            ),
            ("req", r#"ARRAY[]::"Size"[]"#),
            ("req2", r#"ARRAY['SMALL'::"Size"]"#),
            ("appr", r#"ARRAY[]::"Size"[]"#),
            ("arr3", "'{}'::text[]"),
            ("j", r#"'{"SMALL": 0}'::jsonb"#),
            ("j2", "'[]'::jsonb"),
        ],
    );
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn enum_array_columns_survive_v2_commit_and_reopen() {
    let path = common::fresh("v2-enum-array-defaults");
    {
        let db = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
        let mut sql = db.sql_session();
        for statement in [
            r#"CREATE TYPE "Size" AS ENUM ('SMALL','LARGE')"#,
            r#"CREATE TABLE sizes (id int PRIMARY KEY, choices "Size"[] DEFAULT ARRAY['SMALL']::"Size"[])"#,
            "INSERT INTO sizes (id) VALUES (1)",
            r#"ALTER TABLE sizes ADD COLUMN extra "Size"[] NOT NULL DEFAULT ARRAY[]::"Size"[]"#,
        ] {
            run(&mut sql, statement);
        }
    }
    let db = Admin::open(&path, Limits::default()).unwrap();
    let mut sql = db.sql_session();
    run(
        &mut sql,
        r#"UPDATE sizes SET extra = ARRAY['LARGE']::"Size"[] WHERE id=1"#,
    );
    assert_eq!(
        run(
            &mut sql,
            "SELECT choices, extra, pg_typeof(extra) FROM sizes"
        ),
        vec![vec![text("{SMALL}"), text("{LARGE}"), text(r#""Size"[]"#)]],
    );
    error(
        &mut sql,
        "UPDATE sizes SET extra='{NOPE}' WHERE id=1",
        SqlError::InvalidTextRepresentation,
        r#"invalid input value for enum "Size": "NOPE""#,
    );
    defaults(
        &db,
        "sizes",
        &[
            ("choices", r#"ARRAY['SMALL'::"Size"]"#),
            ("extra", r#"ARRAY[]::"Size"[]"#),
        ],
    );
}

fn enum_rows(sql: &mut SqlSession) {
    for statement in [
        "CREATE TYPE wf_mood AS ENUM ('z','a')",
        "CREATE TYPE wf_other AS ENUM ('z','a')",
        "CREATE TABLE wf_rows (id int PRIMARY KEY, a wf_mood[], v wf_mood)",
        "INSERT INTO wf_rows VALUES (1,'{z}','z'),(2,'{a}','a'),(3,'{z,NULL}',NULL),(4,NULL,NULL),(5,'{a,NULL}','a')",
    ] {
        run(sql, statement);
    }
}

#[test]
fn enum_array_expressions_keep_label_order_and_type() {
    let db = Admin::create(common::fresh("enum-array-expressions"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    enum_rows(&mut sql);
    for statement in [
        "SELECT greatest(ARRAY['z']::wf_mood[], ARRAY['a']::wf_mood[]), least(ARRAY['a']::wf_mood[], ARRAY['z']::wf_mood[])",
        "SELECT greatest(a,'{a}'), least(a,'{a}'::wf_mood[]) FROM wf_rows WHERE id = 1",
    ] {
        assert_eq!(run(&mut sql, statement), vec![vec![text("{a}"), text("{z}")]]);
    }
    for statement in [
        "SELECT id FROM wf_rows WHERE id IN (1,2) ORDER BY coalesce(NULL,a,'{a}')",
        "SELECT id FROM wf_rows WHERE id IN (1,2) ORDER BY CASE WHEN id > 0 THEN a ELSE '{z}' END",
        "SELECT id FROM wf_rows WHERE id IN (1,2) ORDER BY nullif(a,NULL)",
        "SELECT id FROM wf_rows WHERE id IN (1,2) ORDER BY first_value(a) OVER (PARTITION BY id)",
        "SELECT id FROM wf_rows WHERE id IN (1,2) ORDER BY coalesce(v,'a')",
        "SELECT id FROM wf_rows WHERE id IN (1,2) ORDER BY CASE WHEN id > 0 THEN v END",
        "SELECT id FROM wf_rows WHERE id IN (1,2) ORDER BY nullif(v,NULL)",
        "SELECT id FROM wf_rows WHERE id IN (1,2) ORDER BY a[1]",
        "SELECT id FROM wf_rows WHERE id IN (1,2) ORDER BY ARRAY[v]",
        "SELECT id FROM wf_rows WHERE id IN (1,2) ORDER BY a || '{a}'",
    ] {
        assert_eq!(run(&mut sql, statement), vec![vec![Value::Int(1)], vec![Value::Int(2)]], "{statement}");
    }
    assert_eq!(
        run(&mut sql, "SELECT pg_typeof(min(coalesce(NULL,a))), min(coalesce(a,a)), max(coalesce(a,a)) FROM wf_rows WHERE id IN (1,2)"),
        vec![vec![text("wf_mood[]"), text("{z}"), text("{a}")]],
    );
    assert_eq!(
        run(&mut sql, "SELECT id, max(a) OVER (ORDER BY id) AS m FROM wf_rows WHERE id IN (1,2) ORDER BY m DESC"),
        vec![vec![Value::Int(2), text("{a}")], vec![Value::Int(1), text("{z}")]],
    );
    assert_eq!(
        run(&mut sql, "SELECT id, pg_typeof(prev), prev FROM (SELECT id, lag(a, 1, '{a}') OVER (ORDER BY id) AS prev FROM wf_rows WHERE id IN (1,2)) s ORDER BY prev"),
        vec![vec![Value::Int(2), text("wf_mood[]"), text("{z}")], vec![Value::Int(1), text("wf_mood[]"), text("{a}")]],
    );
    error(
        &mut sql,
        "SELECT CASE WHEN false THEN a ELSE 'z'::text END FROM wf_rows",
        SqlError::Type,
        "CASE types text and wf_mood[] cannot be matched",
    );
    error(
        &mut sql,
        "SELECT lag(a,1,NULL::wf_other[]) OVER (ORDER BY id) FROM wf_rows",
        SqlError::UndefinedFunction,
        "function lag(wf_mood[], integer, wf_other[]) does not exist",
    );
    assert_eq!(
        run(&mut sql, "SELECT DISTINCT nullif(a,NULL) AS m FROM wf_rows ORDER BY m"),
        vec![vec![text("{z}")], vec![text("{z,NULL}")], vec![text("{a}")], vec![text("{a,NULL}")], vec![Value::Null]],
    );
    assert_eq!(
        run(&mut sql, "SELECT coalesce(a,a) < '{a}', pg_typeof(a[1]), pg_typeof(ARRAY[v,'a']), pg_typeof(nullif(v,NULL)) FROM wf_rows WHERE id=1"),
        vec![vec![Value::Bool(true), text("wf_mood"), text("wf_mood[]"), text("wf_mood")]],
    );
    assert_eq!(
        run(&mut sql, "SELECT pg_typeof(a || v), pg_typeof(v || a), width_bucket('z', ARRAY['z','a']::wf_mood[]) FROM wf_rows WHERE id=1"),
        vec![vec![text("wf_mood[]"), text("wf_mood[]"), Value::Int(1)]],
    );
    assert_eq!(
        run(&mut sql, "SELECT pg_typeof(a), a FROM (VALUES (ARRAY['z']::wf_mood[]), (NULL), ('{a}')) s(a) ORDER BY a"),
        vec![vec![text("wf_mood[]"), text("{z}")], vec![text("wf_mood[]"), text("{a}")], vec![text("wf_mood[]"), Value::Null]],
    );
    assert_eq!(
        run(&mut sql, "SELECT greatest(ARRAY['z'], ARRAY['a']), least(ARRAY[10], ARRAY[2]), min(a::text[]) FROM wf_rows"),
        vec![vec![text("{z}"), text("{2}"), text("{a}")]],
    );
}

#[test]
fn set_operations_use_enum_identity_and_coerce_unknowns() {
    let db = Admin::create(common::fresh("enum-array-sets"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    enum_rows(&mut sql);
    for (operator, context) in [
        ("UNION", "UNION"),
        ("UNION ALL", "UNION"),
        ("INTERSECT", "INTERSECT"),
        ("EXCEPT", "EXCEPT"),
    ] {
        for (left, right, suffix) in [
            ("a", "ARRAY['z']::wf_other[]", "[]"),
            ("v", "'z'::wf_other", ""),
        ] {
            error(
                &mut sql,
                &format!("SELECT {left} FROM wf_rows WHERE false {operator} SELECT {right}"),
                SqlError::CannotCoerce,
                &format!("{context} could not convert type wf_other{suffix} to wf_mood{suffix}"),
            );
        }
        error(
            &mut sql,
            &format!("SELECT a FROM wf_rows WHERE false {operator} SELECT '{{BOGUS}}' WHERE false"),
            SqlError::InvalidTextRepresentation,
            r#"invalid input value for enum wf_mood: "BOGUS""#,
        );
    }
    for (expr, left, literal, ty, first, second) in [
        ("a", "a", "'{a}'", "wf_mood[]", "{z}", "{a}"),
        ("v", "v", "'a'", "wf_mood", "z", "a"),
    ] {
        assert_eq!(
            run(&mut sql, &format!(
                "SELECT pg_typeof({expr}), {expr} FROM (SELECT {left} FROM wf_rows WHERE id=1 UNION ALL SELECT {literal} UNION ALL SELECT NULL) s ORDER BY {expr}"
            )),
            vec![vec![text(ty), text(first)], vec![text(ty), text(second)], vec![text(ty), Value::Null]],
        );
        assert_eq!(
            run(&mut sql, &format!(
                "SELECT pg_typeof(x), x FROM (SELECT NULL AS x UNION ALL SELECT {left} FROM wf_rows WHERE id=1) s ORDER BY x"
            )),
            vec![vec![text(ty), text(first)], vec![text(ty), Value::Null]],
        );
        for (operator, expected) in [("INTERSECT", second), ("EXCEPT", first)] {
            assert_eq!(
                run(&mut sql, &format!(
                    "SELECT pg_typeof({expr}), {expr} FROM (SELECT {left} FROM wf_rows WHERE id IN (1,2) {operator} SELECT {literal}) s"
                )),
                vec![vec![text(ty), text(expected)]],
            );
        }
    }
    error(
        &mut sql,
        "SELECT v FROM wf_rows UNION ALL SELECT 'BOGUS'",
        SqlError::InvalidTextRepresentation,
        r#"invalid input value for enum wf_mood: "BOGUS""#,
    );
    assert_eq!(run(&mut sql, "SELECT count(*) FROM wf_rows"), vec![vec![Value::Int(5)]]);
}

#[test]
fn implicit_enum_literals_are_validated_without_input_rows() {
    let db = Admin::create(common::fresh("enum-array-literal-binding"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    enum_rows(&mut sql);
    for statement in [
        "SELECT '{BOGUS}' = a FROM wf_rows WHERE false",
        "SELECT a = '{BOGUS}' FROM wf_rows WHERE false",
        "SELECT 'BOGUS' = v FROM wf_rows WHERE false",
        "SELECT v = 'BOGUS' FROM wf_rows WHERE false",
        "SELECT coalesce(a,'{BOGUS}') FROM wf_rows WHERE false",
        "SELECT CASE WHEN false THEN '{BOGUS}' ELSE a END FROM wf_rows WHERE false",
        "SELECT nullif(a,'{BOGUS}') FROM wf_rows WHERE false",
        "SELECT greatest(a,'{BOGUS}') FROM wf_rows WHERE false",
        "SELECT a || '{BOGUS}' FROM wf_rows WHERE false",
        "SELECT width_bucket('BOGUS', a) FROM wf_rows WHERE false",
    ] {
        error(
            &mut sql,
            statement,
            SqlError::InvalidTextRepresentation,
            r#"invalid input value for enum wf_mood: "BOGUS""#,
        );
    }
    assert_eq!(
        run(&mut sql, "SELECT id, a, v FROM wf_rows WHERE a = '{z}' ORDER BY id"),
        vec![vec![Value::Int(1), text("{z}"), text("z")]],
    );
}

fn oracle_rows(result: giggles_core::relational::SqlResult) -> Vec<String> {
    result
        .rows
        .into_iter()
        .map(|row| {
            row.values
                .into_iter()
                .map(|value| match value {
                    Value::Null => "<NULL>".into(),
                    Value::Text(text) => text,
                    Value::Int(n) => n.to_string(),
                    Value::Bool(b) => if b { "t" } else { "f" }.into(),
                    other => panic!("unexpected oracle value {other:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
fn prisma_infers_enum_parameter_identity() {
    use giggles_core::relational::{DataType, type_oid};

    let db = Admin::create(common::fresh("prisma-enum-parameters"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    for statement in [
        r#"CREATE TYPE "Status" AS ENUM ('TRIALING','ACTIVE','CANCELED')"#,
        "CREATE TYPE pe AS ENUM ('z','a')",
        r#"CREATE TABLE "Sub"(id int PRIMARY KEY, "status" "Status" NOT NULL DEFAULT 'TRIALING', tags "Status"[] DEFAULT ARRAY[]::"Status"[], a pe[], v pe)"#,
        r#"INSERT INTO "Sub" VALUES (1,'ACTIVE','{ACTIVE}','{z}','z'),(2,'CANCELED',NULL,NULL,NULL)"#,
    ] {
        run(&mut sql, statement);
    }
    let types = sql.user_type_catalog().unwrap().types;
    let oid = |name| {
        type_oid(
            *types
                .iter()
                .find(|(_, definition)| definition.name() == name)
                .unwrap()
                .0,
        )
        .unwrap()
    };
    let (pe, status) = (oid("pe"), oid("Status"));
    let array = (DataType::TextArray, pe + 1);
    let scalar = (DataType::Text, pe);
    let status_scalar = (DataType::Text, status);
    let status_array = (DataType::TextArray, status + 1);
    let integer = (DataType::Integer, 0);
    let sql_text = (DataType::Text, 0);
    type Case<'a> = (&'a str, &'a [(DataType, u32)], &'a [Value], &'a [&'a str]);
    let cases: [Case<'_>; 16] = [
        (
            r#"SELECT coalesce(a,$1) FROM "Sub""#,
            std::slice::from_ref(&array),
            &[text("{a}")],
            &["{z}", "{a}"],
        ),
        (
            r#"SELECT CASE WHEN id=1 THEN a ELSE $1 END FROM "Sub""#,
            std::slice::from_ref(&array),
            &[text("{a}")],
            &["{z}", "{a}"],
        ),
        (
            r#"SELECT nullif(a,$1) FROM "Sub""#,
            std::slice::from_ref(&array),
            &[text("{z}")],
            &["<NULL>", "<NULL>"],
        ),
        (
            r#"SELECT greatest(a,$1), least($2,a) FROM "Sub""#,
            &[array.clone(), array.clone()],
            &[text("{a}"), text("{a}")],
            &["{a}|{z}", "{a}|{a}"],
        ),
        (
            r#"SELECT lag(a,1,$1) OVER (ORDER BY id) FROM "Sub""#,
            std::slice::from_ref(&array),
            &[text("{a}")],
            &["{a}", "{z}"],
        ),
        (
            r#"SELECT coalesce(v,$1), CASE WHEN id=1 THEN v ELSE $2 END, nullif(v,$3), greatest(v,$4), lag(v,1,$5) OVER (ORDER BY id) FROM "Sub""#,
            &[
                scalar.clone(),
                scalar.clone(),
                scalar.clone(),
                scalar.clone(),
                scalar.clone(),
            ],
            &[text("a"), text("a"), text("z"), text("a"), text("a")],
            &["z|z|<NULL>|a|a", "a|a|<NULL>|a|z"],
        ),
        (
            r#"SELECT id FROM "Sub" WHERE "status" = $1"#,
            std::slice::from_ref(&status_scalar),
            &[text("ACTIVE")],
            &["1"],
        ),
        (
            r#"SELECT id FROM "Sub" WHERE "status" IN ($1,$2)"#,
            &[status_scalar.clone(), status_scalar.clone()],
            &[text("ACTIVE"), text("CANCELED")],
            &["1", "2"],
        ),
        (
            r#"SELECT id FROM "Sub" WHERE "status" = ANY($1)"#,
            std::slice::from_ref(&status_array),
            &[text("{ACTIVE,CANCELED}")],
            &["1", "2"],
        ),
        (
            r#"UPDATE "Sub" SET "status" = $1, tags = $2 WHERE id = $3"#,
            &[status_scalar.clone(), status_array.clone(), integer.clone()],
            &[text("CANCELED"), text("{CANCELED,ACTIVE}"), Value::Int(1)],
            &[],
        ),
        (
            r#"INSERT INTO "Sub"(id,"status",tags) VALUES ($1,$2,$3)"#,
            &[integer.clone(), status_scalar.clone(), status_array.clone()],
            &[Value::Int(3), text("TRIALING"), text("{}")],
            &[],
        ),
        (
            r#"SELECT id FROM "Sub" WHERE tags @> $1"#,
            std::slice::from_ref(&status_array),
            &[text("{ACTIVE}")],
            &["1"],
        ),
        (
            r#"SELECT coalesce($1, v) FROM "Sub""#,
            std::slice::from_ref(&scalar),
            &[text("a")],
            &["a", "a"],
        ),
        (
            r#"SELECT CASE WHEN id=1 THEN $1 ELSE v END FROM "Sub""#,
            std::slice::from_ref(&scalar),
            &[text("a")],
            &["a", "<NULL>"],
        ),
        (
            r#"SELECT a FROM "Sub" UNION ALL SELECT $1"#,
            std::slice::from_ref(&array),
            &[text("{a}")],
            &["{z}", "<NULL>", "{a}"],
        ),
        (
            r#"SELECT id FROM "Sub" WHERE "status"::text = $1"#,
            std::slice::from_ref(&sql_text),
            &[text("ACTIVE")],
            &["1"],
        ),
    ];
    let mut prepared = Vec::new();
    for (statement, expected, _, _) in &cases {
        let plan = sql.prepare(statement).unwrap();
        let (desc, enums) = sql
            .describe_with_enum_parameters(&plan)
            .unwrap_or_else(|error| {
                panic!("{statement}: {error:?} ({:?})", take_sql_error_message())
            });
        let inferred = desc
            .parameters
            .into_iter()
            .enumerate()
            .map(|(i, storage)| (storage, enums.get(&i).map_or(0, |ty| ty.oid())))
            .collect::<Vec<_>>();
        assert_eq!(inferred.as_slice(), *expected, "{statement}");
        prepared.push(plan);
    }
    for (i, ((statement, _, values, expected), plan)) in cases.iter().zip(&prepared).enumerate() {
        if i == 9 {
            run(&mut sql, "BEGIN");
        }
        let result = sql
            .execute_prepared(plan, values, None)
            .unwrap_or_else(|error| {
                panic!("{statement}: {error:?} ({:?})", take_sql_error_message())
            });
        if matches!(i, 9 | 10) {
            assert_eq!(result.affected, 1, "{statement}");
        }
        assert_eq!(oracle_rows(result), *expected, "{statement}");
        if i == 10 {
            assert_eq!(
                oracle_rows(
                    sql.execute(
                        r#"SELECT id,"status",tags FROM "Sub" ORDER BY id"#,
                        &[],
                        None
                    )
                    .unwrap()
                ),
                [
                    "1|CANCELED|{CANCELED,ACTIVE}",
                    "2|CANCELED|<NULL>",
                    "3|TRIALING|{}"
                ]
            );
        } else if i == 11 {
            run(&mut sql, "ROLLBACK");
        }
    }
    assert_eq!(
        oracle_rows(
            sql.execute(
                r#"SELECT id,"status",tags FROM "Sub" ORDER BY id"#,
                &[],
                None
            )
            .unwrap()
        ),
        ["1|ACTIVE|{ACTIVE}", "2|CANCELED|<NULL>"]
    );

    for (i, bad, type_name) in [
        (0, vec![text("{BOGUS}")], "pe"),
        (
            5,
            vec![text("BOGUS"), text("a"), text("z"), text("a"), text("a")],
            "pe",
        ),
        (6, vec![text("BOGUS")], r#""Status""#),
        (14, vec![text("{BOGUS}")], "pe"),
    ] {
        assert_eq!(
            sql.execute_prepared(&prepared[i], &bad, None),
            Err(CoreError::Sql(SqlError::InvalidTextRepresentation))
        );
        assert_eq!(
            take_sql_error_message().as_deref(),
            Some(format!("invalid input value for enum {type_name}: \"BOGUS\"").as_str())
        );
    }

    for (statement, value, expected) in [
        (
            r#"SELECT coalesce(v,$1), pg_typeof(coalesce(v,$1)) FROM "Sub" WHERE id=2"#,
            "a",
            &["a|pe"][..],
        ),
        (
            r#"SELECT greatest(a,$1) > '{z}'::pe[] FROM "Sub" WHERE id=1"#,
            "{a}",
            &["t"],
        ),
        (
            r#"SELECT id FROM "Sub" ORDER BY coalesce(v,$1), id"#,
            "a",
            &["1", "2"],
        ),
    ] {
        let plan = sql.prepare(statement).unwrap();
        let result = sql
            .execute_prepared(&plan, &[text(value)], None)
            .unwrap_or_else(|error| {
                panic!("{statement}: {error:?} ({:?})", take_sql_error_message())
            });
        assert_eq!(oracle_rows(result), expected, "{statement}");
    }
}

#[test]
fn parameterized_enum_and_ordinary_array_controls() {
    let db = Admin::create(common::fresh("enum-parameter-controls"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    run(&mut sql, "CREATE TYPE param_e AS ENUM ('z','a')");
    run(
        &mut sql,
        "CREATE TABLE param_rows(id int PRIMARY KEY, a param_e[], v param_e, x int[], y text[])",
    );
    run(
        &mut sql,
        "INSERT INTO param_rows VALUES (1,'{z}','z','{2}','{Z}'),(2,NULL,NULL,NULL,NULL)",
    );

    for (column, value, first) in [("a", "{a}", "{z}"), ("v", "a", "z"), ("x", "{10}", "{2}")] {
        for (projection, tail, input, expected) in [
            (
                format!("coalesce({column},$1)"),
                "WHERE id=2",
                value,
                vec![value],
            ),
            (
                format!("CASE WHEN id=1 THEN {column} ELSE $1 END"),
                "WHERE id=2",
                value,
                vec![value],
            ),
            (
                format!("nullif({column},$1)"),
                "WHERE id=1",
                first,
                vec!["<NULL>"],
            ),
            (
                format!("greatest({column},$1)"),
                "WHERE id=1",
                value,
                vec![value],
            ),
            (
                format!("lag({column},1,$1) OVER (ORDER BY id)"),
                "",
                value,
                vec![value, first],
            ),
            (
                format!("lead({column},1,$1) OVER (ORDER BY id)"),
                "",
                value,
                vec!["<NULL>", value],
            ),
        ] {
            let statement = format!("SELECT {projection} FROM param_rows {tail}");
            let plan = sql.prepare(&statement).unwrap();
            let result = sql
                .execute_prepared(&plan, &[text(input)], None)
                .unwrap_or_else(|error| {
                    panic!("{statement}: {error:?} ({:?})", take_sql_error_message())
                });
            assert_eq!(oracle_rows(result), expected, "{statement}");
        }
    }
    let statement = r#"SELECT greatest(y COLLATE "C",$1) FROM param_rows WHERE id=1"#;
    let plan = sql.prepare(statement).unwrap();
    assert_eq!(
        oracle_rows(sql.execute_prepared(&plan, &[text("{a}")], None).unwrap()),
        ["{a}"]
    );
}

#[test]
fn set_arms_resolve_sorted_unknowns_and_literals_after_stars() {
    let db = Admin::create(common::fresh("enum-set-arms"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    for statement in [
        "CREATE TYPE ue AS ENUM ('z','a')",
        "CREATE TABLE ut(id int PRIMARY KEY, k int, a ue[], s ue)",
        "INSERT INTO ut VALUES (1,10,'{z}','z'),(2,20,'{a}','a')",
        "CREATE TABLE us(id int PRIMARY KEY, k int)",
        "INSERT INTO us VALUES (4,40)",
    ] {
        run(&mut sql, statement);
    }
    for (statement, types) in [
        (
            "SELECT pg_typeof(a), a FROM (SELECT a FROM ut UNION ALL (SELECT '{a}' ORDER BY 1)) q",
            "ue[] and text",
        ),
        (
            "SELECT pg_typeof(a), a FROM (SELECT a FROM ut UNION ALL (SELECT DISTINCT '{a}')) q ORDER BY 2",
            "ue[] and text",
        ),
        (
            "SELECT pg_typeof(a), a FROM (SELECT a FROM ut UNION (SELECT '{a}' FROM us GROUP BY id ORDER BY 1)) q ORDER BY 2",
            "ue[] and text",
        ),
        (
            "SELECT pg_typeof(a), a FROM (SELECT a FROM ut UNION ALL (SELECT '{a}' FROM us GROUP BY 1)) q ORDER BY 2",
            "ue[] and text",
        ),
        (
            "SELECT pg_typeof(s), s FROM (SELECT s FROM ut UNION ALL (SELECT 'a' ORDER BY 1)) q",
            "ue and text",
        ),
        (
            "SELECT a FROM (SELECT a FROM ut UNION ALL (SELECT '{a}' ORDER BY 1)) q WHERE a < '{a}'::ue[]",
            "ue[] and text",
        ),
        (
            "SELECT pg_typeof(x), x FROM ((SELECT '{a}' AS x ORDER BY 1) UNION ALL SELECT a FROM ut) q",
            "text and ue[]",
        ),
        (
            "SELECT pg_typeof(x), x FROM (SELECT x FROM (SELECT '{a}' AS x ORDER BY 1) s UNION ALL SELECT a FROM ut) q",
            "text and ue[]",
        ),
        (
            "SELECT pg_typeof(x), x FROM (SELECT x FROM (SELECT '{a}' AS x) s UNION ALL SELECT a FROM ut) q ORDER BY 2",
            "text and ue[]",
        ),
        (
            "SELECT pg_typeof(t), t FROM (SELECT 1::int AS t UNION ALL (SELECT '2' ORDER BY 1)) q",
            "integer and text",
        ),
    ] {
        error(
            &mut sql,
            statement,
            SqlError::Type,
            &format!("UNION types {types} cannot be matched"),
        );
    }
    for (statement, expected) in [
        (
            "SELECT pg_typeof(a), a FROM (SELECT a FROM ut UNION ALL (SELECT '{a}' LIMIT 1)) q ORDER BY 2",
            &["ue[]|{z}", "ue[]|{a}", "ue[]|{a}"][..],
        ),
        (
            "SELECT pg_typeof(a), a FROM (SELECT a FROM ut UNION ALL (SELECT '{a}' OFFSET 0)) q ORDER BY 2",
            &["ue[]|{z}", "ue[]|{a}", "ue[]|{a}"],
        ),
        (
            "SELECT pg_typeof(s), s FROM (SELECT s FROM ut UNION ALL (SELECT 'a' LIMIT 1)) q ORDER BY 2",
            &["ue|z", "ue|a", "ue|a"],
        ),
        (
            "SELECT a FROM (SELECT a FROM ut WHERE id=1 UNION ALL (SELECT '{a}' LIMIT 1)) q WHERE a < '{a}'::ue[]",
            &["{z}"],
        ),
        (
            "SELECT pg_typeof(t), t FROM (SELECT 'x'::text AS t UNION ALL (SELECT 'y' ORDER BY 1)) q ORDER BY 2",
            &["text|x", "text|y"],
        ),
        (
            "SELECT 1 UNION ALL (SELECT '2' FROM us ORDER BY id)",
            &["1", "2"],
        ),
        (
            "SELECT 1 UNION ALL (SELECT '2' FROM us GROUP BY id)",
            &["1", "2"],
        ),
        (
            "SELECT id,k,a FROM ut UNION ALL SELECT *,'{a}' FROM us ORDER BY 1",
            &["1|10|{z}", "2|20|{a}", "4|40|{a}"],
        ),
        (
            "SELECT id,k,s FROM ut UNION ALL SELECT *,'a' FROM us ORDER BY 1",
            &["1|10|z", "2|20|a", "4|40|a"],
        ),
        (
            "SELECT id,k,s FROM ut UNION ALL SELECT us.*,'a' FROM us ORDER BY 1",
            &["1|10|z", "2|20|a", "4|40|a"],
        ),
        (
            "SELECT id,k,a,s FROM ut UNION ALL SELECT *,'{a}','a' FROM us ORDER BY 1",
            &["1|10|{z}|z", "2|20|{a}|a", "4|40|{a}|a"],
        ),
        (
            "SELECT id,a,k FROM ut UNION ALL SELECT id,'{a}',k FROM us ORDER BY 1",
            &["1|{z}|10", "2|{a}|20", "4|{a}|40"],
        ),
        (
            "SELECT *,'{a}' FROM us UNION ALL SELECT id,k,a FROM ut ORDER BY 1",
            &["1|10|{z}", "2|20|{a}", "4|40|{a}"],
        ),
        (
            "SELECT id,k,a FROM ut INTERSECT SELECT *,'{a}' FROM us",
            &[],
        ),
        (
            "SELECT id,k,a FROM ut EXCEPT SELECT *,'{z}' FROM us ORDER BY 1",
            &["1|10|{z}", "2|20|{a}"],
        ),
    ] {
        let result = sql.execute(statement, &[], None).unwrap_or_else(|error| {
            panic!("{statement}: {error:?} ({:?})", take_sql_error_message())
        });
        assert_eq!(oracle_rows(result), expected, "{statement}");
    }
    for statement in [
        "SELECT id,k,a FROM ut UNION ALL SELECT *,'{BOGUS}' FROM us",
        "SELECT id,k,s FROM ut UNION ALL SELECT *,'BOGUS' FROM us",
    ] {
        error(
            &mut sql,
            statement,
            SqlError::InvalidTextRepresentation,
            r#"invalid input value for enum ue: "BOGUS""#,
        );
    }
}


#[test]
fn set_arm_group_by_prefers_input_names() {
    let db = Admin::create(common::fresh("set-group-input"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    for statement in [
        "CREATE TYPE xe AS ENUM ('z','a')",
        "CREATE TABLE gt(id int PRIMARY KEY, n int, d date, s xe)",
        "INSERT INTO gt VALUES (1,1,'2020-01-01','z')",
        "CREATE TABLE gs(id int PRIMARY KEY, k int)",
        "INSERT INTO gs VALUES (4,40)",
    ] {
        run(&mut sql, statement);
    }
    for (statement, expected) in [
        (
            "SELECT n FROM gt UNION ALL SELECT '3' AS k FROM gs GROUP BY k",
            &["1", "3"][..],
        ),
        (
            "SELECT n FROM gt UNION ALL SELECT '3' AS k FROM gs JOIN gt ON true GROUP BY k",
            &["1", "3"],
        ),
        (
            "SELECT pg_typeof(n), n FROM (SELECT n FROM gt UNION ALL SELECT '3' AS id FROM gs GROUP BY id) q ORDER BY 2",
            &["integer|1", "integer|3"],
        ),
        (
            "SELECT pg_typeof(d), d::text FROM (SELECT d FROM gt UNION ALL SELECT '2021-01-01' AS id FROM gs GROUP BY id) q ORDER BY 2",
            &["date|2020-01-01", "date|2021-01-01"],
        ),
        (
            "SELECT pg_typeof(s), s FROM (SELECT s FROM gt UNION ALL SELECT 'a' AS id FROM gs GROUP BY id) q ORDER BY 2",
            &["xe|z", "xe|a"],
        ),
        (
            "SELECT pg_typeof(s), s FROM (SELECT s FROM gt UNION ALL SELECT 'a' AS k FROM gs GROUP BY k) q ORDER BY 2",
            &["xe|z", "xe|a"],
        ),
    ] {
        let result = sql.execute(statement, &[], None).unwrap_or_else(|error| {
            panic!("{statement}: {error:?} ({:?})", take_sql_error_message())
        });
        assert_eq!(oracle_rows(result), expected, "{statement}");
    }

    for statement in [
        "SELECT n FROM gt UNION ALL SELECT '3' AS zz FROM gs GROUP BY zz",
        "SELECT n FROM gt UNION ALL (SELECT '3' AS k FROM gs ORDER BY k)",
    ] {
        error(
            &mut sql,
            statement,
            SqlError::Type,
            "UNION types integer and text cannot be matched",
        );
    }
}

#[test]
fn set_arm_distinct_on_resolves_referenced_unknown_outputs() {
    let db = Admin::create(common::fresh("set-distinct-on"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    for statement in [
        "CREATE TYPE xe AS ENUM ('z','a')",
        "CREATE TABLE xt(id int PRIMARY KEY, s xe, a xe[], n int)",
        "INSERT INTO xt VALUES (1,'z','{z}',1)",
        "CREATE TABLE xs(id int PRIMARY KEY, k int)",
        "INSERT INTO xs VALUES (4,40)",
    ] {
        run(&mut sql, statement);
    }
    for (statement, types) in [
        (
            "SELECT a FROM (SELECT a FROM xt UNION ALL SELECT DISTINCT ON (1) '{a}') q WHERE a < '{a}'::xe[]",
            "xe[] and text",
        ),
        (
            "SELECT s FROM (SELECT s FROM xt UNION ALL SELECT DISTINCT ON (1) 'a') q WHERE s < 'a'::xe",
            "xe and text",
        ),
        (
            "SELECT pg_typeof(n), n FROM (SELECT n FROM xt UNION ALL SELECT DISTINCT ON (1) '3') q ORDER BY 2",
            "integer and text",
        ),
        (
            "SELECT n FROM xt UNION ALL SELECT DISTINCT ON (target) '3' AS target FROM xs",
            "integer and text",
        ),
        (
            "SELECT n FROM xt UNION ALL SELECT DISTINCT ON (k) '3' AS k FROM xs",
            "integer and text",
        ),
    ] {
        error(
            &mut sql,
            statement,
            SqlError::Type,
            &format!("UNION types {types} cannot be matched"),
        );
    }
    assert_eq!(
        run(
            &mut sql,
            "SELECT pg_typeof(s), s FROM (SELECT s FROM xt UNION ALL SELECT DISTINCT ON (k) 'a' FROM xs) q ORDER BY 2",
        ),
        [vec![text("xe"), text("z")], vec![text("xe"), text("a")]]
    );
}

#[test]
fn enum_array_containment_preserves_enum_types() {
    let db = Admin::create(common::fresh("enum-containment"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    for statement in [
        "CREATE TYPE ce AS ENUM ('z','a')",
        "CREATE TABLE uc (id int PRIMARY KEY, e ce[])",
        "INSERT INTO uc VALUES (1,'{z}')",
    ] {
        run(&mut sql, statement);
    }
    assert_eq!(
        run(&mut sql, "SELECT e @> '{z}', e <@ '{z,a}', e && '{a}', e && e FROM uc"),
        [vec![Value::Bool(true), Value::Bool(true), Value::Bool(false), Value::Bool(true)]]
    );

    for (expression, value) in [("e @> $1", "{z}"), ("e <@ $1", "{z,a}"), ("e && $1", "{z}")] {
        let statement = format!("SELECT {expression} FROM uc");
        let plan = sql.prepare(&statement).unwrap();
        let (description, enums) = sql.describe_with_enum_parameters(&plan).unwrap();
        assert_eq!(description.parameters, [giggles_core::relational::DataType::TextArray]);
        assert!(enums.contains_key(&0), "{statement}");
        let result = sql.execute_prepared(&plan, &[text(value)], None).unwrap();
        assert_eq!(result.rows[0].values, [Value::Bool(true)], "{statement}");
    }
    error(
        &mut sql,
        "SELECT e @> '{BOGUS}' FROM uc",
        SqlError::InvalidTextRepresentation,
        r#"invalid input value for enum ce: "BOGUS""#,
    );
    assert_eq!(run(&mut sql, "SELECT e FROM uc"), [vec![text("{z}")]]);
}

#[test]
fn ordinary_array_containment_stays_unsupported() {
    let db = Admin::create(common::fresh("ordinary-containment"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    run(
        &mut sql,
        "CREATE TABLE ordinary(id int PRIMARY KEY, xi int[], xt text[])",
    );
    for expression in ["xi @> $1", "xt <@ $1", "xt @> xt", "xt @> '{a}'", "xi @> '{1}'"] {
        let statement = format!("SELECT {expression} FROM ordinary");
        let plan = sql.prepare(&statement).unwrap();
        assert!(
            matches!(
                sql.describe_with_enum_parameters(&plan),
                Err(CoreError::Sql(SqlError::Unsupported))
            ),
            "{statement}"
        );
    }
}


fn uuid_array_default_roundtrip(engine: Engine) {
    let path = common::fresh("uuid-array-defaults");
    let default_hint = "You will need to rewrite or cast the expression.";
    {
        let db = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut sql = db.sql_session();
        for statement in [
            "CREATE TABLE pr_a (id int PRIMARY KEY, u UUID[] DEFAULT ARRAY[]::UUID[],
                w uuid[] NOT NULL DEFAULT '{}',
                x uuid[] DEFAULT ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11']::uuid[],
                y uuid[] DEFAULT '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'::uuid[])",
            "CREATE TABLE pr_b (id int PRIMARY KEY, u UUID[])",
            "ALTER TABLE pr_b ALTER COLUMN u SET DEFAULT ARRAY[]::UUID[]",
            "INSERT INTO pr_b (id) VALUES (1)",
            "ALTER TABLE pr_b ADD COLUMN v UUID[] DEFAULT ARRAY[]::UUID[]",
            "ALTER TABLE pr_b ADD COLUMN z UUID[] NOT NULL DEFAULT '{}'",
            "INSERT INTO pr_b (id) VALUES (2)",
            "INSERT INTO pr_a (id) VALUES (1)",
        ] {
            run(&mut sql, statement);
        }
    }

    let db = Admin::open(&path, Limits::default()).unwrap();
    let mut sql = db.sql_session();
    let empty_row = |id| vec![Value::Int(id), text("{}"), text("{}"), text("{}")];
    assert_eq!(
        run(&mut sql, "SELECT id, u, v, z FROM pr_b ORDER BY id"),
        vec![empty_row(1), empty_row(2)],
    );
    let populated = text("{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}");
    assert_eq!(
        run(&mut sql, "SELECT id, u, w, x, y FROM pr_a"),
        vec![vec![Value::Int(1), text("{}"), text("{}"), populated.clone(), populated]],
    );
    assert_eq!(
        run(&mut sql, "SELECT pg_typeof(u), pg_typeof(w), pg_typeof(x), pg_typeof(y) FROM pr_a"),
        vec![vec![text("uuid[]"); 4]],
    );
    defaults(
        &db,
        "pr_a",
        &[
            ("u", "ARRAY[]::uuid[]"),
            ("w", "'{}'::uuid[]"),
            ("x", "ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid]"),
            ("y", "'{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'::uuid[]"),
        ],
    );
    defaults(
        &db,
        "pr_b",
        &[("u", "ARRAY[]::uuid[]"), ("v", "ARRAY[]::uuid[]"), ("z", "'{}'::uuid[]")],
    );

    run(&mut sql, "ALTER TABLE pr_b ALTER COLUMN u DROP DEFAULT");
    let column_id = db.catalog().unwrap().1.iter().find(|t| t.name == "pr_b").unwrap().columns[1].id;
    assert!(!db.catalog_presentation().unwrap().defaults.contains_key(&column_id));
    run(&mut sql, "INSERT INTO pr_b (id) VALUES (3)");
    assert_eq!(
        run(&mut sql, "SELECT id, u, v, z FROM pr_b ORDER BY id"),
        vec![
            empty_row(1),
            empty_row(2),
            vec![Value::Int(3), Value::Null, text("{}"), text("{}")],
        ],
    );

    error(
        &mut sql,
        "CREATE TABLE pr_bad (id int PRIMARY KEY, u uuid[] DEFAULT '{nope}')",
        SqlError::InvalidTextRepresentation,
        r#"invalid input syntax for type uuid: "nope""#,
    );
    assert_eq!(take_sql_error_hint(), None);
    for (statement, message) in [
        (
            "CREATE TABLE pr_bad2 (id int PRIMARY KEY, u uuid[] DEFAULT ARRAY[]::text[])",
            r#"column "u" is of type uuid[] but default expression is of type text[]"#,
        ),
        (
            "CREATE TABLE pr_bad3 (id int PRIMARY KEY, u uuid[] DEFAULT ARRAY[1])",
            r#"column "u" is of type uuid[] but default expression is of type integer[]"#,
        ),
        (
            "ALTER TABLE pr_b ALTER COLUMN u SET DEFAULT ARRAY[1]",
            r#"column "u" is of type uuid[] but default expression is of type integer[]"#,
        ),
        (
            "ALTER TABLE pr_b ADD COLUMN bad uuid[] DEFAULT ARRAY[]::text[]",
            r#"column "bad" is of type uuid[] but default expression is of type text[]"#,
        ),
    ] {
        error(&mut sql, statement, SqlError::Type, message);
        assert_eq!(take_sql_error_hint(), Some(default_hint));
    }
    assert_eq!(
        run(&mut sql, "SELECT id, u, v, z FROM pr_b ORDER BY id"),
        vec![
            empty_row(1),
            empty_row(2),
            vec![Value::Int(3), Value::Null, text("{}"), text("{}")],
        ],
    );
}

#[test]
fn uuid_array_defaults_survive_legacy_reopen() {
    uuid_array_default_roundtrip(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn uuid_array_defaults_survive_v2_reopen() {
    uuid_array_default_roundtrip(Engine::V2);
}

#[test]
fn archdocs_revision_tables_accept_defaults() {
    let db = Admin::create(common::fresh("archdocs-revision-defaults"), Limits::default()).unwrap();
    let mut sql = db.sql_session();
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 1_000_000,
        statement_us: 1_000_000,
        zone: Some(SessionZone::Fixed(0)),
        wall: None,
    };
    for statement in [
        r#"CREATE TYPE "DesignAuthorKind" AS ENUM ('HUMAN', 'AGENT', 'ARCHDOCS_AI')"#,
        r#"CREATE TABLE "DesignDocVariation" ("id" UUID PRIMARY KEY)"#,
        include_str!("fixtures/archdocs_page_revision.sql"),
        include_str!("fixtures/archdocs_design_doc_variation_version.sql"),
        r#"INSERT INTO "DesignDocVariation" VALUES ('00000000-0000-0000-0000-000000000001')"#,
        r#"INSERT INTO "PageRevision" ("pageId", "revision", "title", "content", "authorKind")
           VALUES ('00000000-0000-0000-0000-000000000001', 1, 'First', 'Body', 'HUMAN')"#,
        r#"INSERT INTO "DesignDocVariationVersion"
             ("variationId", "version", "content", "brief", "authorKind", "authorUserIds")
           VALUES ('00000000-0000-0000-0000-000000000001', 1, 'Body', '{}', 'AGENT', '{}')"#,
    ] {
        sql.execute_at(statement, &[], None, &clock)
            .unwrap_or_else(|error| panic!("{statement}: {error:?} ({:?})", take_sql_error_message()));
    }

    assert_eq!(
        run(&mut sql, r#"SELECT "authorUserIds", "id" IS NOT NULL, "createdAt" IS NOT NULL FROM "PageRevision""#),
        vec![vec![text("{}"), Value::Bool(true), Value::Bool(true)]],
    );
    assert_eq!(
        run(&mut sql, r#"SELECT "authorUserIds", "id" IS NOT NULL, "createdAt" IS NOT NULL, "isAmendment"
                          FROM "DesignDocVariationVersion""#),
        vec![vec![text("{}"), Value::Bool(true), Value::Bool(true), Value::Bool(false)]],
    );
}
