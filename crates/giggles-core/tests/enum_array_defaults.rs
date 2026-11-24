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
