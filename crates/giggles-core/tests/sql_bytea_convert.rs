mod common;
use giggles_core::{
    Admin, Limits, SqlSession,
    relational::{
        SqlError, SqlResult, StatementClock, Value, take_sql_error_hint, take_sql_error_message,
    },
    sqlstate,
};

const CLOCK: StatementClock = StatementClock {
    unavailable: None,
    transaction_us: 850_000_000_000_000,
    statement_us: 850_000_000_000_000,
    zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
    wall: None,
};

fn execute(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute_at(sql, &[], None, &CLOCK)
        .unwrap_or_else(|error| panic!("{sql}: {error:?} ({:?})", take_sql_error_message()))
}

fn select(session: &mut SqlSession, expression: &str) -> Vec<Value> {
    execute(session, &format!("SELECT {expression}"))
        .rows
        .remove(0)
        .values
}

fn text(value: &str) -> Value {
    Value::Text(value.into())
}

fn error(
    session: &mut SqlSession,
    expression: &str,
    code: &str,
    message: &str,
    hint: Option<&str>,
) {
    let error = session
        .execute(&format!("SELECT {expression}"), &[], None)
        .unwrap_err();
    assert_eq!(sqlstate(&error), code, "{expression}: {error:?}");
    assert_eq!(
        take_sql_error_message().as_deref(),
        Some(message),
        "{expression}"
    );
    assert_eq!(take_sql_error_hint(), hint, "{expression}");
}

#[test]
fn utf8_and_sql_ascii_conversions() {
    let admin = Admin::create(common::fresh("bytea-convert"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for encoding in ["UTF8", "utf8", "utf-8", "Utf8", "Unicode", "SQL_ASCII"] {
        let sql = format!(
            r#"convert_to('héllo', '{encoding}'), convert_from('\x68c3a96c6c6f', '{encoding}')"#
        );
        assert_eq!(
            select(&mut session, &sql),
            vec![Value::Bytes("héllo".as_bytes().to_vec()), text("héllo")]
        );
    }
    assert_eq!(
        select(
            &mut session,
            "pg_typeof(convert_to('a', 'UTF8')), pg_typeof(convert_from('a', 'UTF8'))"
        ),
        vec![text("bytea"), text("text")]
    );
    assert_eq!(
        select(
            &mut session,
            "convert_to(NULL, 'UTF8'), convert_to('a', NULL), convert_from(NULL, 'UTF8'), convert_from('a', NULL)"
        ),
        vec![Value::Null; 4]
    );
    assert_eq!(
        select(
            &mut session,
            "convert_to('', 'UTF8'), convert_from('', 'SQL_ASCII')"
        ),
        vec![Value::Bytes(Vec::new()), text("")]
    );
    execute(
        &mut session,
        "CREATE TABLE encoding_values (id int PRIMARY KEY, b bytea, t text)",
    );
    assert_eq!(
        execute(
            &mut session,
            r"INSERT INTO encoding_values VALUES (1, convert_to('a', 'SQL_ASCII'), convert_from('\x61', 'UTF8'))"
        ).affected,
        1
    );
    assert_eq!(
        select(&mut session, "b, t FROM encoding_values"),
        vec![Value::Bytes(b"a".to_vec()), text("a")]
    );
    for (expression, code, message) in [
        (
            "convert_to('a', 'nosuch')",
            "22023",
            r#"invalid destination encoding name "nosuch""#,
        ),
        (
            r"convert_from('\x61', 'nosuch')",
            "22023",
            r#"invalid source encoding name "nosuch""#,
        ),
        (
            "convert_to('a', 'LATIN1')",
            "0A000",
            r#"encoding conversion from "UTF8" to "LATIN1" is not supported"#,
        ),
        (
            r"convert_from('\x61', 'iso-8859-1')",
            "0A000",
            r#"encoding conversion from "LATIN1" to "UTF8" is not supported"#,
        ),
        (
            "convert_to('a', 'win-949')",
            "0A000",
            r#"encoding conversion from "UTF8" to "UHC" is not supported"#,
        ),
        (
            r"convert_from('\xff', 'UTF8')",
            "22021",
            r#"invalid byte sequence for encoding "UTF8": 0xff"#,
        ),
        (
            r"convert_from('\x6100', 'UTF8')",
            "22021",
            r#"invalid byte sequence for encoding "UTF8": 0x00"#,
        ),
        (
            r"convert_from('\xe20062', 'UTF8')",
            "22021",
            r#"invalid byte sequence for encoding "UTF8": 0xe2 0x00 0x62"#,
        ),
        (
            r"convert_from('\x00', 'SQL_ASCII')",
            "22021",
            r#"invalid byte sequence for encoding "SQL_ASCII": 0x00"#,
        ),
        (
            r"convert_from('\xff00', 'sql-ascii')",
            "22021",
            r#"invalid byte sequence for encoding "SQL_ASCII": 0x00"#,
        ),
        (
            r"convert_from('\xff', 'SQL_ASCII')",
            "22021",
            r#"invalid byte sequence for encoding "UTF8": 0xff"#,
        ),
    ] {
        error(&mut session, expression, code, message, None);
    }
}

#[test]
fn bytea_codecs_keep_values_and_postgresql_errors() {
    let admin = Admin::create(common::fresh("bytea-codecs"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for (expression, expected) in [
        (r"encode('\x00ff10'::bytea, 'HEX')", text("00ff10")),
        ("encode('a'::bytea, 'Base64')", text("YQ==")),
        (r"encode('\x80'::bytea, 'escape')", text(r"\200")),
        ("decode(E'00\tff', 'hex')", Value::Bytes(vec![0, 255])),
        (
            r"decode(E'aGVs\nbG8=', 'base64')",
            Value::Bytes(b"hello".to_vec()),
        ),
        (r"decode('\\', 'escape')", Value::Bytes(b"\\".to_vec())),
        (r"decode('a\\9', 'escape')", Value::Bytes(b"a\\9".to_vec())),
        ("encode(NULL::bytea, 'hex')", Value::Null),
        (r"encode('\x00'::bytea, NULL)", Value::Null),
        ("decode(NULL, 'hex')", Value::Null),
        ("decode('00', NULL)", Value::Null),
    ] {
        assert_eq!(
            select(&mut session, expression),
            vec![expected],
            "{expression}"
        );
    }
    for (bytes, length) in [(55, 76), (56, 76), (57, 77), (58, 81), (114, 154)] {
        assert_eq!(
            select(
                &mut session,
                &format!("length(encode(convert_to(repeat('x', {bytes}), 'UTF8'), 'base64'))")
            ),
            vec![Value::Int(length)]
        );
    }
    let truncation =
        Some("Input data is missing padding, is truncated, or is otherwise corrupted.");
    for (expression, message, hint) in [
        (
            r"encode('\x00'::bytea, 'bogus')",
            r#"unrecognized encoding: "bogus""#,
            None,
        ),
        (
            "decode('', 'bogus')",
            r#"unrecognized encoding: "bogus""#,
            None,
        ),
        (
            r"encode('\x00'::bytea, 'base64url')",
            r#"unrecognized encoding: "base64url""#,
            None,
        ),
        (
            "decode('_-8', 'base64url')",
            r#"unrecognized encoding: "base64url""#,
            None,
        ),
        (
            "decode('0g', 'hex')",
            r#"invalid hexadecimal digit: "g""#,
            None,
        ),
        (
            "decode('0é', 'hex')",
            r#"invalid hexadecimal digit: "é""#,
            None,
        ),
        (
            "decode('abc', 'hex')",
            "invalid hexadecimal data: odd number of digits",
            None,
        ),
        (
            "decode('aGVsbG8', 'base64')",
            "invalid base64 end sequence",
            truncation,
        ),
        (
            "decode('=', 'base64')",
            r#"unexpected "=" while decoding base64 sequence"#,
            None,
        ),
        (
            "decode('a===', 'base64')",
            r#"unexpected "=" while decoding base64 sequence"#,
            None,
        ),
        (
            "decode('a!bc', 'base64')",
            r#"invalid symbol "!" found while decoding base64 sequence"#,
            None,
        ),
    ] {
        error(&mut session, expression, "22023", message, hint);
    }
    for input in [r"a\1", r"a\12", r"a\400"] {
        error(
            &mut session,
            &format!("decode('{input}', 'escape')"),
            "22P02",
            "invalid input syntax for type bytea",
            None,
        );
    }
}

#[test]
fn json_array_values_variadic_and_result_types() {
    let admin = Admin::create(common::fresh("json-array-builder"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for (expression, expected) in [
        (
            r#"jsonb_build_array(1, 'a', NULL, true, 1.50, '{"b":2,"a":1}'::jsonb, ARRAY[1,2])::text"#,
            r#"[1, "a", null, true, 1.50, {"a": 1, "b": 2}, [1, 2]]"#,
        ),
        ("jsonb_build_array()::text", "[]"),
        ("json_build_array()::text", "[]"),
        ("jsonb_build_array(NULL)::text", "[null]"),
        ("json_build_array(1,'a',NULL)::text", r#"[1, "a", null]"#),
        (
            r#"jsonb_build_array('é "q" \ /'::text, E'tab\there')::text"#,
            r#"["é \"q\" \\ /", "tab\there"]"#,
        ),
        (
            r#"json_build_array('é "q" \ /'::text, E'nl\nx', E'\u0001')"#,
            r#"["é \"q\" \\ /", "nl\nx", "\u0001"]"#,
        ),
        (
            r#"jsonb_build_array(1::int2, 2.0::float8, 'NaN'::float8, 1e30::numeric, '2026-01-02'::date, '2026-01-02 03:04:05'::timestamp, '{"a":1}'::json, '{1,NULL}'::int[], 'x'::varchar, true)::text"#,
            r#"[1, 2, "NaN", 1000000000000000000000000000000, "2026-01-02", "2026-01-02T03:04:05", {"a": 1}, [1, null], "x", true]"#,
        ),
        (
            r#"json_build_array(1.50, '{"b":2,"a":1}'::jsonb, '{"b":2, "a":1}'::json, ARRAY['a','b'], NULL)::text"#,
            r#"[1.50, {"a": 1, "b": 2}, {"b":2, "a":1}, ["a","b"], null]"#,
        ),
        (
            "jsonb_build_array(VARIADIC ARRAY[1,2,3])::text",
            "[1, 2, 3]",
        ),
        ("json_build_array(VARIADIC '{}'::int[])::text", "[]"),
        (
            r#"json_build_array(VARIADIC ARRAY['{"a":1}'::json, '[1,  2]'::json, NULL::json])::text"#,
            r#"[{"a":1}, [1,  2], null]"#,
        ),
        (
            "json_build_array(VARIADIC ARRAY[json_build_array(1,'x')])::text",
            r#"[[1, "x"]]"#,
        ),
        (
            r#"jsonb_build_array(VARIADIC ARRAY['{"a":1}'::json])::text"#,
            r#"[{"a": 1}]"#,
        ),
        (
            "json_build_array(VARIADIC ARRAY['{}'::json, '[]', NULL])::text",
            "[{}, [], null]",
        ),
        (
            "json_build_array(VARIADIC ARRAY['a',NULL,'b'])",
            r#"["a", null, "b"]"#,
        ),
        ("json_build_array(json_build_array(1,2))", "[[1, 2]]"),
        ("jsonb_build_array(json_build_array(1,2))", "[[1, 2]]"),
        ("pg_catalog.jsonb_build_array('a') -> 0", r#""a""#),
        ("jsonb_build_array(1,'x') ->> 1", "x"),
        ("jsonb_build_array('a', 'b')::text || ''", r#"["a", "b"]"#),
    ] {
        assert_eq!(
            select(&mut session, expression),
            vec![text(expected)],
            "{expression}"
        );
    }
    assert_eq!(
        select(
            &mut session,
            "pg_typeof(jsonb_build_array(1)), pg_typeof(json_build_array(1))"
        ),
        vec![text("jsonb"), text("json")]
    );
    assert_eq!(
        select(
            &mut session,
            "jsonb_build_array(VARIADIC NULL::int[]), json_build_array(VARIADIC NULL::text[])"
        ),
        vec![Value::Null; 2]
    );
    assert_eq!(
        select(&mut session, r#"jsonb_build_array('a') = '["a"]'::jsonb"#),
        vec![Value::Bool(true)]
    );
    for sql in [
        "SELECT json_build_array(VARIADIC '{{1,2},{3,4}}'::int[])",
        "SELECT json_build_array(VARIADIC NULL::json[])",
    ] {
        assert_eq!(
            session.execute(sql, &[], None),
            Err(giggles_core::CoreError::Sql(SqlError::Unsupported)),
            "{sql}"
        );
    }
    assert_eq!(
        session.execute("SELECT jsonb_build_array(ROW(1,'a'))", &[], None),
        Err(giggles_core::CoreError::Sql(SqlError::Unsupported))
    );
}

#[test]
fn linear_outbox_migration_matches_postgresql_keys_and_deduplication() {
    let admin = Admin::create(common::fresh("bytea-convert-migration"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let migration = include_str!("fixtures/am8-linear-outbox.sql");
    let results = migration
        .split_terminator(';')
        .filter(|statement| !statement.trim().is_empty())
        .map(|statement| execute(&mut session, statement).affected)
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 7);
    assert_eq!(&results[4..6], &[10, 2]);

    let keys = [
        "33c412376d7d9009a318578afd2fd032fe9af7c5a2254d9cba61ee1f29a4ecd7",
        "15d4a74e4ca3193dac41bdf4e0da432dbad5c794ae4d361496cf4b6e5a478038",
        "5ccaea7721e208a00bcd00e4307f1a7421bbd11d37bb79114d46848d8d707e86",
        "bfe0500032be76b9e397e0a9aa0a1807b234c91ee5bfe442353efaccc1d1c6d3",
        "657fc3b40985262aee68a715214f225e8f3e671fe280efd50fab0a8d462d5cbf",
        "55f6371958d307000f34db59dcb5e3586ccde6aa1f36aa8c85254833003e862d",
    ];
    let rows = execute(
        &mut session,
        r#"SELECT "status", "dedupeKey", "lockedUntil" IS NOT NULL, "lastError",
                  "updatedAt" > '2026-06-01'
           FROM "LinearOutbox" ORDER BY "id""#,
    )
    .rows;
    assert_eq!(rows.len(), 11);
    for (index, (status, key)) in [
        ("PENDING", Some(0)),
        ("DONE", Some(0)),
        ("PENDING", Some(1)),
        ("PENDING", Some(2)),
        ("DONE", Some(2)),
        ("PENDING", Some(2)),
        ("DONE", Some(3)),
        ("PENDING", Some(3)),
        ("PENDING", None),
        ("PENDING", Some(4)),
        ("PENDING", Some(5)),
    ]
    .into_iter()
    .enumerate()
    {
        let last_error = if matches!(index, 1 | 4) {
            text("Duplicate pending link job")
        } else {
            Value::Null
        };
        assert_eq!(
            rows[index].values,
            vec![
                text(status),
                key.map_or(Value::Null, |i| text(keys[i])),
                Value::Bool(index == 0),
                last_error,
                Value::Bool(matches!(index, 1 | 4)),
            ],
            "row {}",
            index + 1
        );
    }
    let duplicate = session.execute_at(
        &format!(r#"INSERT INTO "LinearOutbox" ("installationId","kind","payload","dedupeKey","updatedAt")
                     VALUES ('10000000-0000-0000-0000-000000000001','LINK_ISSUE','{{}}','{}', now())"#, keys[2]),
        &[],
        None,
        &CLOCK,
    ).unwrap_err();
    assert_eq!(sqlstate(&duplicate), "23505");
}
