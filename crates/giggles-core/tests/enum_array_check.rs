mod common;

use giggles_core::{
    Admin, Limits, SqlSession,
    relational::{
        SqlResult, Value, take_sql_error_detail, take_sql_error_hint, take_sql_error_message,
    },
};

fn execute(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?} {:?}", take_sql_error_message()))
}

fn error(session: &mut SqlSession, sql: &str, code: &str, message: &str, detail: Option<&str>) {
    let error = session.execute(sql, &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), code, "{sql}: {error:?}");
    assert_eq!(take_sql_error_message().as_deref(), Some(message), "{sql}");
    assert_eq!(take_sql_error_detail().as_deref(), detail, "{sql}");
}

fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    execute(session, sql)
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

#[test]
fn enum_quantifiers_in_select_and_mutations() {
    use Value::{Bool, Int, Null, Text};

    let admin = Admin::create(common::fresh("enum-quantifiers"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    execute(
        &mut s,
        r#"CREATE TYPE "Size" AS ENUM ('SMALL', 'STANDARD', 'LARGE')"#,
    );
    execute(
        &mut s,
        r#"CREATE TABLE vt (id int PRIMARY KEY, sizes "Size"[], opt "Size"[], counts jsonb)"#,
    );
    execute(
        &mut s,
        "INSERT INTO vt (id, sizes, opt) VALUES (1, '{SMALL,LARGE}', NULL), (2, '{}', '{STANDARD,NULL}'), (3, '{STANDARD}', '{NULL}'), (4, '{LARGE}', '{}')",
    );
    let result = execute(
        &mut s,
        "SELECT id, 'SMALL' = ANY(sizes) AS s, 'STANDARD' = ANY(sizes) AS st, 'SMALL' <> ALL(sizes) AS ns FROM vt ORDER BY id",
    );
    assert_eq!(result.columns, ["id", "s", "st", "ns"]);
    assert_eq!(
        result
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        [
            vec![Int(1), Bool(true), Bool(false), Bool(false)],
            vec![Int(2), Bool(false), Bool(false), Bool(true)],
            vec![Int(3), Bool(false), Bool(true), Bool(true)],
            vec![Int(4), Bool(false), Bool(false), Bool(true)],
        ]
    );
    assert_eq!(
        rows(
            &mut s,
            "SELECT id, 'SMALL' = ANY(opt), 'STANDARD' = ANY(opt), 'SMALL' <> ALL(opt), 'STANDARD' <> ALL(opt) FROM vt ORDER BY id"
        ),
        [
            vec![Int(1), Null, Null, Null, Null],
            vec![Int(2), Null, Bool(true), Null, Bool(false)],
            vec![Int(3), Null, Null, Null, Null],
            vec![Int(4), Bool(false), Bool(false), Bool(true), Bool(true)],
        ]
    );
    for (sql, ids) in [
        (
            "SELECT id FROM vt WHERE 'LARGE' = ANY(sizes) ORDER BY id",
            &[1, 4][..],
        ),
        (
            "SELECT id FROM vt WHERE 'LARGE' <> ALL(sizes) ORDER BY id",
            &[2, 3],
        ),
        (
            "SELECT id FROM vt WHERE sizes[1] = ANY('{LARGE,STANDARD}') ORDER BY id",
            &[3, 4],
        ),
        (
            "SELECT id FROM vt WHERE 'SMALL' < SOME(sizes) ORDER BY id",
            &[1, 3, 4],
        ),
        (
            "SELECT id FROM vt WHERE 'SMALL' = SOME(sizes) ORDER BY id",
            &[1],
        ),
        (
            "SELECT id FROM vt WHERE 'SMALL' IN ('SMALL') AND 'SMALL' = ANY(sizes) ORDER BY id",
            &[1],
        ),
    ] {
        assert_eq!(
            rows(&mut s, sql),
            ids.iter().map(|id| vec![Int(*id)]).collect::<Vec<_>>(),
            "{sql}"
        );
    }
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT 'SMALL' = ANY(NULL::"Size"[]), 'SMALL' <> ALL(NULL::"Size"[])"#
        ),
        [vec![Null, Null]]
    );
    for sql in [
        "SELECT 'HUGE' = ANY(sizes) FROM vt",
        "SELECT 'HUGE' <> ALL(sizes) FROM vt",
        "UPDATE vt SET counts=jsonb_build_object('X', CASE WHEN 'HUGE' = ANY(sizes) THEN 1 ELSE 0 END)",
    ] {
        error(
            &mut s,
            sql,
            "22P02",
            r#"invalid input value for enum "Size": "HUGE""#,
            None,
        );
    }
    for (sql, message) in [
        (
            "SELECT 'SMALL'::text = ANY(sizes) FROM vt",
            r#"operator does not exist: text = "Size""#,
        ),
        (
            "SELECT sizes = ANY(ARRAY['SMALL']) FROM vt",
            r#"operator does not exist: "Size"[] = text"#,
        ),
        (
            "SELECT 'SMALL' LIKE ANY(sizes) FROM vt",
            r#"operator does not exist: unknown ~~ "Size""#,
        ),
    ] {
        error(&mut s, sql, "42883", message, None);
    }
    let object = "jsonb_build_object('SMALL', CASE WHEN 'SMALL' = ANY(sizes) THEN 1 ELSE 0 END, 'LARGE', CASE WHEN 'LARGE' = ANY(sizes) THEN 1 ELSE 0 END)";
    let expected = [
        r#"{"LARGE": 1, "SMALL": 1}"#,
        r#"{"LARGE": 0, "SMALL": 0}"#,
        r#"{"LARGE": 0, "SMALL": 0}"#,
        r#"{"LARGE": 1, "SMALL": 0}"#,
    ]
    .map(|value| vec![Text(value.into())]);
    assert_eq!(
        rows(
            &mut s,
            &format!("SELECT {object} AS counts FROM vt ORDER BY id")
        ),
        expected
    );
    assert_eq!(
        execute(&mut s, &format!("UPDATE vt SET counts = {object}")).affected,
        4
    );
    assert_eq!(rows(&mut s, "SELECT counts FROM vt ORDER BY id"), expected);
    execute(
        &mut s,
        r#"CREATE TABLE copied (id int PRIMARY KEY, sizes "Size"[])"#,
    );
    assert_eq!(
        execute(
            &mut s,
            "INSERT INTO copied SELECT id, sizes FROM vt WHERE 'SMALL' = ANY(sizes)"
        )
        .affected,
        1
    );
    assert_eq!(rows(&mut s, "SELECT id FROM copied"), [vec![Int(1)]]);
    assert_eq!(
        rows(
            &mut s,
            "INSERT INTO vt (id, sizes) VALUES (1, '{}') ON CONFLICT (id)
        DO UPDATE SET counts = '{}' WHERE 'SMALL' = ANY(vt.sizes) RETURNING id"
        ),
        [vec![Int(1)]]
    );
    assert_eq!(
        rows(
            &mut s,
            "UPDATE copied SET id = 5 WHERE 'LARGE' <> ANY(sizes) RETURNING id"
        ),
        [vec![Int(5)]]
    );
    assert_eq!(
        rows(
            &mut s,
            "DELETE FROM copied WHERE 'SMALL' < ANY(sizes) RETURNING id"
        ),
        [vec![Int(5)]]
    );
    assert_eq!(
        execute(&mut s, "DELETE FROM vt WHERE 'STANDARD' = ANY(sizes)").affected,
        1
    );
    assert_eq!(
        rows(&mut s, "SELECT id FROM vt ORDER BY id"),
        [vec![Int(1)], vec![Int(2)], vec![Int(4)]]
    );
}

#[test]
fn enum_text_cast_is_rejected_in_partial_index_predicates() {
    let admin = Admin::create(common::fresh("enum-partial-index"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    execute(&mut s, "CREATE TYPE kind AS ENUM ('A', 'B')");
    execute(
        &mut s,
        "CREATE TABLE items (id int PRIMARY KEY, k kind CHECK (k::text IN ('A', 'B')))",
    );
    for cast in ["text", "varchar(3)"] {
        error(
            &mut s,
            &format!("CREATE INDEX items_k ON items (id) WHERE k::{cast} IN ('A', 'B')"),
            "42P17",
            "functions in index predicate must be marked IMMUTABLE",
            None,
        );
    }
    execute(&mut s, "CREATE INDEX items_k ON items (id) WHERE k = 'A'");
}

#[test]
fn jsonb_build_object_errors_match_evaluation_and_key_types() {
    let admin = Admin::create(
        common::fresh("jsonb-build-object-errors"),
        Limits::default(),
    )
    .unwrap();
    let mut s = admin.sql_session();
    for sql in [
        r#"SELECT jsonb_build_object('{"a":1}'::json, 1)"#,
        r#"SELECT jsonb_build_object('{"a":1}'::jsonb, 1)"#,
        "SELECT jsonb_build_object(ARRAY[1], 1)",
        "SELECT jsonb_build_object(ROW(1, 'a'), 1)",
    ] {
        error(
            &mut s,
            sql,
            "22023",
            "key value must be scalar, not array, composite, or json",
            None,
        );
    }
    for (sql, message) in [
        (
            "SELECT jsonb_build_object(NULL::json, 1)",
            "argument 1: key must not be null",
        ),
        (
            "SELECT jsonb_build_object('a', 1, NULL, 2)",
            "argument 3: key must not be null",
        ),
    ] {
        error(&mut s, sql, "22023", message, None);
    }
    assert_eq!(
        rows(
            &mut s,
            "SELECT CASE WHEN false THEN jsonb_build_object('a') END AS v"
        ),
        [vec![Value::Null]]
    );
    error(
        &mut s,
        "SELECT jsonb_build_object('a', 1, 'b')",
        "22023",
        "argument list must have even number of elements",
        None,
    );
    assert_eq!(
        take_sql_error_hint(),
        Some("The arguments of jsonb_build_object() must consist of alternating keys and values.")
    );
    execute(
        &mut s,
        "CREATE TABLE objects (id int PRIMARY KEY, obj jsonb)",
    );
    error(
        &mut s,
        "INSERT INTO objects SELECT 1, jsonb_build_object('a')",
        "22023",
        "argument list must have even number of elements",
        None,
    );
}

#[test]
fn enum_text_checks_restore_from_postgresql_definitions() {
    let admin = Admin::create(common::fresh("enum-text-check-restore"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    execute(&mut s, "CREATE TYPE kind AS ENUM ('A', 'B', 'C', 'CX')");
    let cases = [
        (
            "k::text IN ('A', 'B')",
            "CHECK (((k)::text = ANY (ARRAY['A'::text, 'B'::text])))",
        ),
        (
            "k::text <> ALL (ARRAY['A', 'B'])",
            "CHECK (((k)::text <> ALL (ARRAY['A'::text, 'B'::text])))",
        ),
        (
            "k::text = SOME (ARRAY['A', 'B'])",
            "CHECK (((k)::text = ANY (ARRAY['A'::text, 'B'::text])))",
        ),
        (
            "k::text = ANY ('{A,B}'::text[])",
            "CHECK (((k)::text = ANY ('{A,B}'::text[])))",
        ),
        (
            "lower(k::text) IN ('a', 'b')",
            "CHECK ((lower((k)::text) = ANY (ARRAY['a'::text, 'b'::text])))",
        ),
        (
            "k::varchar(20) IN ('A', 'B')",
            "CHECK ((((k)::character varying(20))::text = ANY ((ARRAY['A'::character varying, 'B'::character varying])::text[])))",
        ),
        (
            "k::varchar IN ('A', 'B', 'C')",
            "CHECK ((((k)::character varying)::text = ANY ((ARRAY['A'::character varying, 'B'::character varying, 'C'::character varying])::text[])))",
        ),
        (
            "CAST(k AS varchar(1)) <> 'C'",
            "CHECK ((((k)::character varying(1))::text <> 'C'::text))",
        ),
        (
            "((k)::character varying(20))::text = ANY (ARRAY[('A'::character varying)::text, ('B'::character varying)::text])",
            "CHECK ((((k)::character varying(20))::text = ANY (ARRAY[('A'::character varying)::text, ('B'::character varying)::text])))",
        ),
    ];
    for (i, (expression, definition)) in cases.into_iter().enumerate() {
        execute(
            &mut s,
            &format!(
                "CREATE TABLE original_{i} (id int, k kind, CONSTRAINT original_check_{i} CHECK ({expression}))"
            ),
        );
        execute(
            &mut s,
            &format!("CREATE TABLE restored_{i} (id int, k kind)"),
        );
        execute(
            &mut s,
            &format!("ALTER TABLE restored_{i} ADD CONSTRAINT restored_check_{i} {definition}"),
        );
        execute(
            &mut s,
            &format!(
                "CREATE TABLE copied_{i} (id int, k kind, CONSTRAINT copied_check_{i} {definition})"
            ),
        );
        let checks = admin.catalog_presentation().unwrap().constraints;
        for name in ["original", "restored", "copied"].map(|prefix| format!("{prefix}_check_{i}")) {
            let check = checks.iter().find(|check| check.name == name).unwrap();
            assert_eq!(
                format!("CHECK ({})", check.expression.as_deref().unwrap()),
                definition
            );
        }
        let bad_label = if expression.contains("<> ALL") {
            "A"
        } else {
            "CX"
        };
        error(
            &mut s,
            &format!("INSERT INTO restored_{i} VALUES (1, '{bad_label}')"),
            "23514",
            &format!(
                r#"new row for relation "restored_{i}" violates check constraint "restored_check_{i}""#
            ),
            Some(&format!("Failing row contains (1, {bad_label}).")),
        );
    }
    execute(&mut s, "CREATE TABLE existing (id int, k kind)");
    execute(&mut s, "INSERT INTO existing VALUES (1, 'C')");
    error(
        &mut s,
        "ALTER TABLE existing ADD CONSTRAINT existing_check CHECK (((k)::text = ANY (ARRAY['A'::text, 'B'::text])))",
        "23514",
        r#"check constraint "existing_check" of relation "existing" is violated by some row"#,
        None,
    );
}

#[test]
fn enum_value_rename_leaves_check_text_literals_intact() {
    let admin = Admin::create(common::fresh("enum-check-rename"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    execute(&mut s, "CREATE TYPE kind AS ENUM ('A', 'B', 'C')");
    execute(
        &mut s,
        "CREATE TABLE items (id int PRIMARY KEY, k kind, CONSTRAINT text_check CHECK (k::text IN ('A','B')))",
    );
    execute(&mut s, "INSERT INTO items VALUES (1, 'A')");
    execute(
        &mut s,
        "CREATE TABLE enum_key (k kind PRIMARY KEY CHECK (k::text = 'A'))",
    );
    execute(&mut s, "INSERT INTO enum_key VALUES ('A')");
    execute(&mut s, "ALTER TYPE kind RENAME VALUE 'A' TO 'Z'");
    assert_eq!(
        rows(&mut s, "SELECT k FROM items"),
        [vec![Value::Text("Z".into())]]
    );
    assert_eq!(
        rows(&mut s, "SELECT k FROM enum_key"),
        [vec![Value::Text("Z".into())]]
    );
    let check = admin
        .catalog_presentation()
        .unwrap()
        .constraints
        .into_iter()
        .find(|check| check.name == "text_check")
        .unwrap();
    assert_eq!(
        check.expression.as_deref(),
        Some("((k)::text = ANY (ARRAY['A'::text, 'B'::text]))")
    );
    error(
        &mut s,
        "INSERT INTO items VALUES (2, 'Z')",
        "23514",
        r#"new row for relation "items" violates check constraint "text_check""#,
        Some("Failing row contains (2, Z)."),
    );
}

#[test]
fn add_column_checks_existing_heap_rows_with_the_default() {
    let admin = Admin::create(common::fresh("heap-add-column-check"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    execute(&mut s, "CREATE TYPE kind AS ENUM ('A', 'B')");
    execute(&mut s, "CREATE TABLE heap (id int)");
    execute(&mut s, "INSERT INTO heap VALUES (1)");
    for (name, definition) in [
        (
            "bad_int",
            "int DEFAULT 0 CONSTRAINT bad_int CHECK (bad_int > 0)",
        ),
        (
            "bad_enum",
            "kind DEFAULT 'B' CONSTRAINT bad_enum CHECK (bad_enum::text IN ('A'))",
        ),
    ] {
        error(
            &mut s,
            &format!("ALTER TABLE heap ADD COLUMN {name} {definition}"),
            "23514",
            &format!(r#"check constraint "{name}" of relation "heap" is violated by some row"#),
            None,
        );
    }
    execute(
        &mut s,
        "ALTER TABLE heap ADD COLUMN n int DEFAULT 1 CONSTRAINT good_int CHECK (n > 0)",
    );
    execute(
        &mut s,
        "ALTER TABLE heap ADD COLUMN k kind DEFAULT 'A' CONSTRAINT good_enum CHECK (k::text IN ('A'))",
    );
    assert_eq!(
        rows(&mut s, "SELECT * FROM heap"),
        [vec![Value::Int(1), Value::Int(1), Value::Text("A".into())]]
    );
    error(
        &mut s,
        "INSERT INTO heap VALUES (2, 0, 'A')",
        "23514",
        r#"new row for relation "heap" violates check constraint "good_int""#,
        Some("Failing row contains (2, 0, A)."),
    );
    error(
        &mut s,
        "INSERT INTO heap VALUES (2, 1, 'B')",
        "23514",
        r#"new row for relation "heap" violates check constraint "good_enum""#,
        Some("Failing row contains (2, 1, B)."),
    );
}

#[test]
fn enum_text_checks_validate_and_survive_reopen() {
    let path = common::fresh("enum-text-check");
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        for sql in [
            r#"CREATE TYPE "Kind" AS ENUM ('UPDATES', 'REPLACES', 'RELATES')"#,
            r#"CREATE TABLE rel (id int PRIMARY KEY, "toDocId" int, "toAdrId" int, "toAdrRevisionId" int, kind "Kind" NOT NULL)"#,
            r#"INSERT INTO rel VALUES (1, 10, NULL, NULL, 'UPDATES'), (2, 11, NULL, NULL, 'REPLACES')"#,
            r#"ALTER TYPE "Kind" ADD VALUE 'IMPLEMENTS'"#,
            r#"ALTER TYPE "Kind" ADD VALUE 'RELIES_ON'"#,
            r#"ALTER TABLE rel ADD COLUMN "toAdrRevisionAccepted" boolean NOT NULL DEFAULT false"#,
            r#"ALTER TABLE rel ADD CONSTRAINT rel_target_check CHECK (("toDocId" IS NOT NULL AND "toAdrId" IS NULL AND "toAdrRevisionId" IS NULL AND NOT "toAdrRevisionAccepted" AND kind::text IN ('UPDATES','REPLACES')) OR ("toDocId" IS NULL AND "toAdrId" IS NOT NULL AND kind::text IN ('IMPLEMENTS','RELIES_ON')))"#,
            r#"INSERT INTO rel VALUES (3, NULL, 7, NULL, 'IMPLEMENTS', false)"#,
            r#"CREATE TABLE rel2 (id int PRIMARY KEY, kind "Kind")"#,
            r#"INSERT INTO rel2 VALUES (1, 'UPDATES')"#,
            r#"CREATE TABLE rel3 (id int PRIMARY KEY, k "Kind", CONSTRAINT rel3_k CHECK (k::text IN ('UPDATES','RELATES')), CHECK (CAST(k AS text) <> 'REPLACES'), CHECK (k::varchar(20) NOT IN ('RELIES_ON')))"#,
            r#"INSERT INTO rel3 VALUES (1, 'UPDATES'), (2, NULL)"#,
            r#"ALTER TABLE rel3 ADD CONSTRAINT rel3_eq CHECK (k = 'UPDATES' OR k IS NULL)"#,
        ] {
            execute(&mut s, sql);
        }
        error(
            &mut s,
            "ALTER TABLE rel2 ADD CONSTRAINT rel2_target_check CHECK (kind::text IN ('IMPLEMENTS','RELIES_ON'))",
            "23514",
            r#"check constraint "rel2_target_check" of relation "rel2" is violated by some row"#,
            None,
        );
        execute(&mut s, "INSERT INTO rel2 VALUES (2, 'UPDATES')");
    }
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for (sql, detail) in [
        (
            "INSERT INTO rel VALUES (4, NULL, 7, NULL, 'UPDATES', false)",
            "Failing row contains (4, null, 7, null, UPDATES, f).",
        ),
        (
            "UPDATE rel SET kind = 'RELIES_ON' WHERE id = 1",
            "Failing row contains (1, 10, null, null, RELIES_ON, f).",
        ),
    ] {
        error(
            &mut s,
            sql,
            "23514",
            r#"new row for relation "rel" violates check constraint "rel_target_check""#,
            Some(detail),
        );
    }
    execute(
        &mut s,
        r#"UPDATE rel SET kind = 'RELIES_ON', "toDocId" = NULL, "toAdrId" = 8 WHERE id = 1"#,
    );
    error(
        &mut s,
        "INSERT INTO rel3 VALUES (3, 'REPLACES')",
        "23514",
        r#"new row for relation "rel3" violates check constraint "rel3_k""#,
        Some("Failing row contains (3, REPLACES)."),
    );
    assert_eq!(
        rows(&mut s, "SELECT k FROM rel3 ORDER BY id"),
        [vec![Value::Text("UPDATES".into())], vec![Value::Null],]
    );
}
