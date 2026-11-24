mod common;

use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{DataType, Value, take_sql_error_detail, take_sql_error_message},
    sqlstate,
};

const A: &str = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
const B: &str = "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12";
const C: &str = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13";
const D: &str = "c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14";
const ID: &str = "00000000-0000-0000-0000-00000000000";

fn text(s: impl Into<String>) -> Value {
    Value::Text(s.into())
}

fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?} ({:?})", take_sql_error_message()))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}

fn error(s: &mut SqlSession, sql: &str, code: &str) {
    let e = s.execute(sql, &[], None).unwrap_err();
    assert_eq!(sqlstate(&e), code, "{sql}: {e:?}");
}

fn ids(s: &mut SqlSession, sql: &str, expected: &[u8]) {
    assert_eq!(
        run(s, sql),
        expected
            .iter()
            .map(|i| vec![text(format!("{ID}{i}"))])
            .collect::<Vec<_>>(),
        "{sql}"
    );
}

fn application_roundtrip(engine: Engine) {
    let path = common::fresh("uuid-array");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for sql in [
        r#"CREATE TABLE "Ver" (id uuid PRIMARY KEY, "authorUserIds" uuid[] NOT NULL,
               "approverUserIds" uuid[] NOT NULL, "evaluationIds" uuid[])"#,
        r#"CREATE TABLE "Sup" (id uuid PRIMARY KEY, "criterionVersionIds" uuid[], "changeTypeIds" uuid[] NOT NULL)"#,
        r#"ALTER TABLE "Ver" ADD COLUMN "changeTypeIds" uuid[]"#,
        r#"ALTER TABLE "Sup" ADD COLUMN "evaluationIds" uuid[] NOT NULL"#,
    ] {
        run(&mut s, sql);
    }
    let upper = B.to_ascii_uppercase();
    let unhyphenated = A.replace('-', "");
    run(
        &mut s,
        &format!(
            r#"INSERT INTO "Ver" (id,"authorUserIds","approverUserIds") VALUES
            ('{ID}1', ARRAY[]::uuid[], '{{}}'),
            ('{ID}2', ARRAY['{A}'::uuid, '{upper}'], ARRAY['{A}']::uuid[]),
            ('{ID}3', '{{{unhyphenated}}}', '{{NULL}}'),
            ('{ID}4', '{{"{{{C}}}"}}', ARRAY[NULL::uuid,'{D}'])"#,
        ),
    );
    run(
        &mut s,
        &format!(
            r#"INSERT INTO "Sup" VALUES ('{ID}9', ARRAY['{ID}1'::text]::uuid[], '{{}}', '{{}}'::text::uuid[])"#
        ),
    );
    run(&mut s, r#"ALTER TABLE "Ver" ADD COLUMN "otherIds" uuid[]"#);
    assert_eq!(
        run(&mut s, r#"SELECT "otherIds" FROM "Ver" ORDER BY id"#),
        vec![vec![Value::Null]; 4]
    );
    assert_eq!(
        run(
            &mut s,
            r#"SELECT "authorUserIds","approverUserIds" FROM "Ver" ORDER BY id"#
        ),
        vec![
            vec![text("{}"), text("{}")],
            vec![text(format!("{{{A},{B}}}")), text(format!("{{{A}}}"))],
            vec![text(format!("{{{A}}}")), text("{NULL}")],
            vec![text(format!("{{{C}}}")), text(format!("{{NULL,{D}}}"))],
        ]
    );
    for (predicate, expected) in [
        (format!("'{A}'::uuid = ANY(\"authorUserIds\")"), &[2, 3][..]),
        (
            format!("'{A}'::uuid = ANY(\"approverUserIds\"::uuid[])"),
            &[2][..],
        ),
        (format!("'{A}'::uuid <> ALL(\"approverUserIds\")"), &[1][..]),
        (
            format!("\"authorUserIds\" @> ARRAY['{A}']::uuid[]"),
            &[2, 3][..],
        ),
        (
            format!("\"authorUserIds\" <@ ARRAY['{A}','{B}']::uuid[]"),
            &[1, 2, 3][..],
        ),
        (format!("\"authorUserIds\" && '{{{B}}}'"), &[2][..]),
    ] {
        ids(
            &mut s,
            &format!(r#"SELECT id FROM "Ver" WHERE {predicate} ORDER BY 1"#),
            expected,
        );
    }
    assert_eq!(
        run(
            &mut s,
            r#"SELECT "authorUserIds" = '{}', cardinality("authorUserIds"), array_length("authorUserIds",1) FROM "Ver" ORDER BY id"#
        ),
        vec![
            vec![Value::Bool(true), Value::Int(0), Value::Null],
            vec![Value::Bool(false), Value::Int(2), Value::Int(2)],
            vec![Value::Bool(false), Value::Int(1), Value::Int(1)],
            vec![Value::Bool(false), Value::Int(1), Value::Int(1)],
        ]
    );
    assert_eq!(
        run(
            &mut s,
            r#"SELECT "authorUserIds"[1], "authorUserIds"[2], "authorUserIds"[3] FROM "Ver" ORDER BY id LIMIT 1 OFFSET 1"#
        ),
        vec![vec![text(A), text(B), Value::Null]]
    );
    assert_eq!(
        run(
            &mut s,
            r#"SELECT v.id, a.id FROM "Ver" v CROSS JOIN LATERAL unnest(v."authorUserIds") AS a(id) ORDER BY 1,2"#
        ),
        [(2, A), (2, B), (3, A), (4, C)]
            .map(|(id, uuid)| vec![text(format!("{ID}{id}")), text(uuid)])
            .to_vec()
    );
    assert_eq!(
        run(
            &mut s,
            r#"SELECT s.id, v.id FROM "Sup" s JOIN "Ver" v ON v.id = ANY(s."criterionVersionIds"::uuid[]) ORDER BY 1,2"#
        ),
        vec![vec![text(format!("{ID}9")), text(format!("{ID}1"))]]
    );
    let operators = r#"SELECT "authorUserIds" < "approverUserIds", "authorUserIds" = "approverUserIds",
        "authorUserIds" <> "approverUserIds", "authorUserIds" <= "approverUserIds",
        "authorUserIds" > "approverUserIds", "authorUserIds" >= "approverUserIds" FROM "Ver" ORDER BY id"#;
    let comparisons = [
        [false, true, false, true, false, true],
        [false, false, true, false, true, true],
        [true, false, true, true, false, false],
        [true, false, true, true, false, false],
    ]
    .map(|row| row.into_iter().map(Value::Bool).collect::<Vec<_>>());
    assert_eq!(run(&mut s, operators), comparisons);
    ids(
        &mut s,
        r#"SELECT id FROM "Ver" ORDER BY "authorUserIds", id"#,
        &[1, 3, 2, 4],
    );
    let distinct = [
        text("{}"),
        text(format!("{{{A}}}")),
        text("{NULL}"),
        text(format!("{{NULL,{D}}}")),
    ];
    assert_eq!(
        run(
            &mut s,
            r#"SELECT DISTINCT "approverUserIds" FROM "Ver" ORDER BY 1"#
        ),
        distinct
            .iter()
            .cloned()
            .map(|v| vec![v])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        run(
            &mut s,
            r#"SELECT "approverUserIds",count(*) FROM "Ver" GROUP BY "approverUserIds" ORDER BY 1"#
        ),
        distinct
            .iter()
            .cloned()
            .map(|v| vec![v, Value::Int(1)])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        run(
            &mut s,
            r#"SELECT min("approverUserIds"),max("approverUserIds") FROM "Ver""#
        ),
        vec![vec![distinct[0].clone(), distinct[3].clone()]]
    );
    assert_eq!(
        run(
            &mut s,
            &format!(
                r#"SELECT pg_typeof("authorUserIds"), "authorUserIds"::text,
            '{{{A}}}'::uuid[]::text[], ARRAY['{A}']::text[]::uuid[] FROM "Ver" WHERE id='{ID}2'"#
            )
        ),
        vec![vec![
            text("uuid[]"),
            text(format!("{{{A},{B}}}")),
            text(format!("{{{A}}}")),
            text(format!("{{{A}}}"))
        ]]
    );
    for (expression, element) in [
        ("'{not-a-uuid}'", "not-a-uuid"),
        ("'{x}'::text[]::uuid[]", "x"),
    ] {
        error(
            &mut s,
            &format!(
                r#"INSERT INTO "Ver" (id,"authorUserIds","approverUserIds") VALUES ('{ID}5','{{}}',{expression})"#
            ),
            "22P02",
        );
        assert_eq!(
            take_sql_error_message(),
            Some(format!("invalid input syntax for type uuid: \"{element}\""))
        );
    }
    for expression in ["ARRAY[1,2]", "'{}'::text"] {
        error(
            &mut s,
            &format!(
                r#"INSERT INTO "Ver" (id,"authorUserIds","approverUserIds") VALUES ('{ID}5','{{}}',{expression})"#
            ),
            "42804",
        );
    }
    error(
        &mut s,
        &format!(
            r#"INSERT INTO "Ver" (id,"authorUserIds","approverUserIds") VALUES ('{ID}5','{{}}',NULL)"#
        ),
        "23502",
    );
    assert_eq!(
        take_sql_error_message().as_deref(),
        Some(
            r#"null value in column "approverUserIds" of relation "Ver" violates not-null constraint"#
        )
    );
    assert_eq!(
        take_sql_error_detail(),
        Some(format!(
            "Failing row contains ({ID}5, {{}}, null, null, null, null)."
        ))
    );
    error(&mut s, "SELECT '{{1}}'::int[]", "22P02");
    error(&mut s, &format!("SELECT '{{{{{A}}}}}'::uuid[]"), "22P02");
    for f in ["array_append", "array_remove"] {
        error(
            &mut s,
            &format!(r#"SELECT {f}("authorUserIds", '{A}'::uuid) FROM "Ver""#),
            "0A000",
        );
    }
    run(
        &mut s,
        r#"UPDATE "Ver" SET "evaluationIds" = ARRAY[]::uuid[] WHERE "evaluationIds" IS NULL"#,
    );
    assert_eq!(
        run(&mut s, r#"SELECT "evaluationIds" FROM "Ver" ORDER BY id"#),
        vec![vec![text("{}")]; 4]
    );
    assert_eq!(
        run(&mut s, "SELECT ARRAY[]::uuid[], '{}'::uuid[]"),
        vec![vec![text("{}"), text("{}")]]
    );
    drop(s);
    drop(admin);

    let admin = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(
        admin
            .catalog()
            .unwrap()
            .1
            .iter()
            .find(|t| t.name == "Ver")
            .unwrap()
            .columns[1]
            .data_type,
        DataType::UuidArray
    );
    let mut s = admin.sql_session();
    assert_eq!(run(&mut s, operators), comparisons);
    ids(
        &mut s,
        &format!(r#"SELECT id FROM "Ver" WHERE '{A}'::uuid = ANY("authorUserIds") ORDER BY id"#),
        &[2, 3],
    );
    assert_eq!(
        run(&mut s, r#"SELECT "evaluationIds" FROM "Ver" ORDER BY id"#),
        vec![vec![text("{}")]; 4]
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn uuid_array_application_legacy() {
    application_roundtrip(Engine::Legacy);
}

#[test]
#[cfg(feature = "engine-rocksdb")]
fn uuid_array_application_v2() {
    application_roundtrip(Engine::V2);
}

#[test]
fn uuid_array_migration_defaults() {
    let path = common::fresh("uuid-array-defaults");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for sql in [
        "CREATE TABLE versions(id int PRIMARY KEY, authors uuid[] DEFAULT ARRAY[]::uuid[])",
        "INSERT INTO versions(id) VALUES (1)",
        "ALTER TABLE versions ADD COLUMN approvers uuid[] NOT NULL DEFAULT ARRAY[]::uuid[]",
        "ALTER TABLE versions ALTER COLUMN authors SET DEFAULT ARRAY[]::uuid[]",
        "INSERT INTO versions(id) VALUES (2)",
    ] {
        run(&mut s, sql);
    }
    assert_eq!(
        run(
            &mut s,
            "SELECT authors, approvers FROM versions ORDER BY id"
        ),
        vec![vec![text("{}"), text("{}")]; 2]
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn review_array_errors(engine: Engine) {
    use giggles_core::relational::take_sql_error_hint;

    let path = common::fresh("uuid-array-review-errors");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut session = admin.sql_session();
    run(&mut session, "CREATE TABLE u (id int PRIMARY KEY)");
    run(&mut session, "INSERT INTO u VALUES (1)");
    for (array, element) in [("text", "text"), ("int", "integer")] {
        for (operator, quantifier) in [("=", "ANY"), ("<>", "ALL"), ("=", "ALL")] {
            let predicate = format!("'{ID}1'::uuid {operator} {quantifier}('{{}}'::{array}[])");
            for query in [
                format!("SELECT id FROM u WHERE {predicate}"),
                format!("SELECT x.id FROM u x JOIN u y ON {predicate}"),
            ] {
                error(&mut session, &query, "42883");
                assert_eq!(take_sql_error_message(), Some(format!("operator does not exist: uuid {operator} {element}")), "{query}");
                assert_eq!(take_sql_error_hint(), Some("No operator matches the given name and argument types. You might need to add explicit type casts."), "{query}");
            }
        }
    }
    for (predicate, matches) in [
        (format!("= ANY('{{NULL,{ID}1}}'::uuid[])"), true),
        (format!("<> ALL('{{NULL,{ID}1}}'::uuid[])"), false),
        (format!("= ALL('{{NULL,{ID}1}}'::uuid[])"), false),
        ("= ALL('{}'::uuid[])".into(), true),
        ("<> ALL('{}'::uuid[])".into(), true),
    ] {
        assert_eq!(
            run(&mut session, &format!("SELECT x.id FROM u x JOIN u y ON '{ID}1'::uuid {predicate}")),
            if matches { vec![vec![Value::Int(1)]] } else { vec![] },
        );
    }
    for query in [
        format!("SELECT ARRAY[ARRAY['{ID}1'::uuid]]::uuid[]"),
        format!("SELECT '{{{{{ID}1}}}}'::uuid[]"),
    ] {
        error(&mut session, &query, "22P02");
        assert_eq!(take_sql_error_message(), Some(format!("malformed array literal: \"{{{{{ID}1}}}}\"")));
    }
}

#[test]
fn review_array_errors_legacy() {
    review_array_errors(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn review_array_errors_v2() {
    review_array_errors(Engine::V2);
}

#[test]
fn uuid_array_containment_accepts_columns_literals_and_untyped_parameters() {
    let path = common::fresh("uuid-array-containment");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    run(&mut session, "CREATE TABLE uc (id int PRIMARY KEY, v uuid[])");
    let literal = format!("{{{A}}}");
    run(&mut session, &format!("INSERT INTO uc VALUES (1, '{literal}')"));

    assert_eq!(
        run(&mut session, "SELECT id, v = v, v @> v, v <@ v, v && v FROM uc ORDER BY v, id"),
        [vec![Value::Int(1), Value::Bool(true), Value::Bool(true), Value::Bool(true), Value::Bool(true)]]
    );
    assert_eq!(
        run(
            &mut session,
            &format!("SELECT v @> '{literal}', '{literal}' <@ v, v && '{{{B}}}' FROM uc"),
        ),
        [vec![Value::Bool(true), Value::Bool(true), Value::Bool(false)]]
    );

    for expression in ["v @> $1", "$1 <@ v", "v && $1"] {
        let statement = format!("SELECT {expression} FROM uc");
        let plan = session.prepare(&statement).unwrap();
        assert_eq!(session.describe(&plan).unwrap().parameters, [DataType::UuidArray]);
        let result = session.execute_prepared(&plan, &[text(&literal)], None).unwrap();
        assert_eq!(result.rows[0].values, [Value::Bool(true)], "{statement}");
    }

    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
