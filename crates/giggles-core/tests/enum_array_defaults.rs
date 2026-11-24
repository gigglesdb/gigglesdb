mod common;

use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, Value, take_sql_error_message},
};

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
