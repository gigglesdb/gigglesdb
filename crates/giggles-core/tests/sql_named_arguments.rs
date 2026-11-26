//! PostgreSQL 18.6 named-argument oracle cases.
mod common;
use giggles_core::{
    Admin, Limits, SqlSession,
    relational::{DataType, Value, take_sql_error_hint, take_sql_error_message_with_context},
    sqlstate,
};

const AMBIGUOUS_HINT: &str =
    "Could not choose a best candidate function. You might need to add explicit type casts.";
const HINT: &str = "No function matches the given name and argument types. You might need to add explicit type casts.";

fn session() -> SqlSession {
    Admin::create(common::fresh("named-arguments"), Limits::default())
        .unwrap()
        .sql_session()
}

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn result(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn error(s: &mut SqlSession, sql: &str, code: &str, message: &str) {
    let error = s.execute(sql, &[], None).expect_err(sql);
    assert_eq!(sqlstate(&error), code, "{sql}: {error:?}");
    let (actual, _) = take_sql_error_message_with_context();
    assert_eq!(actual.as_deref(), Some(message), "{sql}: {error:?}");
    assert_eq!(
        take_sql_error_hint(),
        match code {
            "42883" => Some(HINT),
            "42725" => Some(AMBIGUOUS_HINT),
            _ => None,
        },
        "{sql}"
    );
}

#[test]
fn temporal_named_values_and_column_labels() {
    let mut s = session();
    for (call, expected) in [
        ("make_interval(days => 7)", "7 days"),
        ("make_interval(days := 7)", "7 days"),
        ("make_interval(1, days => 7)", "1 year 7 days"),
        (
            "make_interval(years => 1, months => 2, weeks => 3, days => 4, hours => 5, mins => 6, secs => 7.5)",
            "1 year 2 mons 25 days 05:06:07.5",
        ),
        ("make_interval(secs => 1.5, days => 2)", "2 days 00:00:01.5"),
        ("make_interval(DAYS => 7)", "7 days"),
        (r#"make_interval("days" => 7)"#, "7 days"),
        ("make_interval(days => '3')", "3 days"),
        (r#"make_interval(days => 1, "years" => 2)"#, "2 years 1 day"),
        (
            "make_interval(secs => 1) + interval '1 day'",
            "1 day 00:00:01",
        ),
        (
            "make_date(year => 2024, month => 2, day => 29)",
            "2024-02-29",
        ),
        (
            "make_date(day => 29, month => 2, year => 2024)",
            "2024-02-29",
        ),
        ("make_date(2024, day => 29, month => 2)", "2024-02-29"),
        ("make_time(hour => 8, min => 15, sec => 23.5)", "08:15:23.5"),
        ("make_time(1, 2, sec => 3)", "01:02:03"),
        (
            "make_timestamp(year => 2024, month => 1, mday => 2, hour => 3, min => 4, sec => 5.5)",
            "2024-01-02 03:04:05.5",
        ),
        (
            "make_timestamptz(2024, 1, 2, 3, 4, 5.5, timezone => 'UTC')",
            "2024-01-02 03:04:05.5+00",
        ),
        (
            "make_timestamptz(2024, 1, 2, 3, 4, 5.5, timezone := 'UTC')",
            "2024-01-02 03:04:05.5+00",
        ),
        (
            "make_timestamptz(year => 2024, month => 1, mday => 2, hour => 3, min => 4, sec => 5.5, timezone => 'Europe/Paris')",
            "2024-01-02 02:04:05.5+00",
        ),
    ] {
        assert_eq!(
            result(&mut s, &format!("SELECT ({call})::text")),
            [[text(expected)]],
            "{call}"
        );
    }
    assert_eq!(
        result(&mut s, "SELECT make_interval(days => null)"),
        [[Value::Null]]
    );
    for (query, name) in [
        ("SELECT make_interval(days => 1)", "make_interval"),
        ("SELECT make_interval(days => 1) AS c", "c"),
    ] {
        let prepared = s.prepare(query).unwrap();
        assert_eq!(s.describe(&prepared).unwrap().columns[0].name, name);
    }
}

#[test]
fn regexp_jsonb_volatile_and_from_calls() {
    let mut s = session();
    for (sql, expected) in [
        (
            r#"SELECT regexp_replace('abcabc', 'b', 'X', start => 1, "N" => 2)"#,
            text("abcaXc"),
        ),
        (
            "SELECT regexp_replace(string => 'abc', pattern => 'b', replacement => 'X')",
            text("aXc"),
        ),
        (
            "SELECT regexp_count(string => 'abcabc', pattern => 'b')",
            Value::Int(2),
        ),
        (
            "SELECT regexp_like('ABC', 'b', flags => 'i')",
            Value::Bool(true),
        ),
        (
            "SELECT regexp_match(string => 'abc', pattern => '(b)(c)')",
            text("{b,c}"),
        ),
        ("SELECT regexp_substr('abcabc', 'b', start => 3)", text("b")),
        (
            "SELECT regexp_split_to_array(string => 'a,b', pattern => ',')",
            text("{a,b}"),
        ),
        (
            "SELECT regexp_replace('abc','b','X',start => '1')",
            text("aXc"),
        ),
        (
            r#"SELECT jsonb_set('{"a":1}', '{a}', '2', create_if_missing => false)"#,
            text(r#"{"a": 2}"#),
        ),
        (
            r#"SELECT jsonb_set(jsonb_in => '{"a":1}', path => '{b}', replacement => '2')"#,
            text(r#"{"a": 1, "b": 2}"#),
        ),
        ("SELECT random(max => 10, min => 10)", Value::Int(10)),
        (
            "SELECT random(min => 1, max => 10) BETWEEN 1 AND 10",
            Value::Bool(true),
        ),
        (
            "SELECT uuidv7(shift => interval '0') IS NOT NULL",
            Value::Bool(true),
        ),
    ] {
        assert_eq!(result(&mut s, sql), [[expected]], "{sql}");
    }
    for (sql, expected) in [
        (
            "SELECT * FROM regexp_matches(string => 'abab', pattern => 'b', flags => 'g')",
            vec![vec![text("{b}")], vec![text("{b}")]],
        ),
        (
            "SELECT * FROM regexp_split_to_table(pattern => ',', string => 'a,b')",
            vec![vec![text("a")], vec![text("b")]],
        ),
        (
            "SELECT token FROM ts_debug(document => 'xyz')",
            vec![vec![text("xyz")]],
        ),
        (
            "SELECT tokid, token FROM ts_parse(txt => 'xyz', parser_name => 'default')",
            vec![vec![Value::Int(1), text("xyz")]],
        ),
        (
            "SELECT tokid, token FROM ts_parse(parser_oid => '3722', txt => 'xyz')",
            vec![vec![Value::Int(1), text("xyz")]],
        ),
        (
            "SELECT alias FROM ts_token_type(parser_name => 'default') WHERE tokid = 1",
            vec![vec![text("asciiword")]],
        ),
        (
            r#"SELECT * FROM jsonb_each(from_json => '{"a":1}')"#,
            vec![vec![text("a"), text("1")]],
        ),
        (
            "SELECT * FROM unnest(tsvector => 'a:1 b:2'::tsvector)",
            vec![
                vec![text("a"), text("{1}"), text("{D}")],
                vec![text("b"), text("{2}"), text("{D}")],
            ],
        ),
    ] {
        assert_eq!(result(&mut s, sql), expected, "{sql}");
    }
}

#[test]
fn diagnostics_render_original_names_types_and_order() {
    let mut s = session();
    for (call, signature) in [
        (
            r#"make_interval("DAYS" => 7)"#,
            "make_interval(DAYS => integer)",
        ),
        ("make_interval(dayz => 7)", "make_interval(dayz => integer)"),
        (
            "make_interval(1, years => 2)",
            "make_interval(integer, years => integer)",
        ),
        (
            "make_interval(weeks => 1.5)",
            "make_interval(weeks => numeric)",
        ),
        (
            "make_interval(days => 2147483648)",
            "make_interval(days => bigint)",
        ),
        (
            "make_interval(VARIADIC days => 1)",
            "make_interval(days => integer)",
        ),
        (
            "make_date(year => 2024, month => 2)",
            "make_date(year => integer, month => integer)",
        ),
        ("lower(x => 'A')", "lower(x => unknown)"),
        ("abs(1, x => 2)", "abs(integer, x => integer)"),
        ("now(x => 1)", "now(x => integer)"),
        (
            "regexp_replace('abcabc', 'b', 'X', start => 1, n => 2)",
            "regexp_replace(unknown, unknown, unknown, start => integer, n => integer)",
        ),
        (
            r#"regexp_instr('abcabc', 'b', "N" => 2)"#,
            "regexp_instr(unknown, unknown, N => integer)",
        ),
        ("count(x => 1)", "count(x => integer)"),
        ("addf(1, d => 2)", "addf(integer, d => integer)"),
    ] {
        error(
            &mut s,
            &format!("SELECT {call}"),
            "42883",
            &format!("function {signature} does not exist"),
        );
    }
    error(
        &mut s,
        "SELECT * FROM ts_parse(parser_name => 3722, txt => 'xyz')",
        "42883",
        "function ts_parse(parser_name => integer, txt => unknown) does not exist",
    );
}

#[test]
fn named_argument_syntax_has_postgres_diagnostics() {
    let mut s = session();
    for (call, message) in [
        (
            "make_interval(days => 7, 1)",
            "positional argument cannot follow named argument",
        ),
        (
            "make_interval(days => 1, days => 2)",
            r#"argument name "days" used more than once"#,
        ),
        (
            "addf(a => 1, a => 2)",
            r#"argument name "a" used more than once"#,
        ),
        (
            "nf(b => 2, 1)",
            "positional argument cannot follow named argument",
        ),
        ("make_interval(=> 1)", r#"syntax error at or near "=>""#),
        ("make_interval(days =>)", r#"syntax error at or near ")""#),
        ("make_interval(1 => 2)", r#"syntax error at or near "=>""#),
        (
            "make_interval(days => 1 => 2)",
            r#"syntax error at or near "=>""#,
        ),
        ("coalesce(a => 1)", r#"syntax error at or near "=>""#),
        (
            "greatest(a => 1, b => 2)",
            r#"syntax error at or near "=>""#,
        ),
        (
            r#"jsonb_extract_path(VARIADIC path_elems => array['a','b'], from_json => '{}')"#,
            r#"syntax error at or near ",""#,
        ),
    ] {
        error(&mut s, &format!("SELECT {call}"), "42601", message);
    }
}

#[test]
fn matching_aggregates_reject_named_arguments() {
    let mut s = session();
    for sql in [
        "SELECT string_agg(value => x, delimiter => ',') FROM (VALUES ('a'),('b')) v(x)",
        "SELECT string_agg(x, delimiter => ',') FROM (VALUES ('a'),('b')) v(x)",
        "SELECT json_object_agg(key => k, value => v) FROM (VALUES ('a',1)) t(k,v)",
    ] {
        error(
            &mut s,
            sql,
            "0A000",
            "aggregates cannot use named arguments",
        );
    }
}

#[test]
fn parameter_metadata_and_prepared_update() {
    let mut s = session();
    result(
        &mut s,
        r#"CREATE TABLE "Interview" ("id" int PRIMARY KEY, "endedAt" timestamptz, "retentionPurgeAt" timestamptz)"#,
    );
    result(
        &mut s,
        r#"INSERT INTO "Interview" ("id", "endedAt") VALUES (1, '2024-01-01 00:00:00+00')"#,
    );
    let update = s.prepare(r#"UPDATE "Interview" SET "retentionPurgeAt" = "endedAt" + make_interval(days => $1::int) WHERE "id" = $2 RETURNING "retentionPurgeAt""#).unwrap();
    assert_eq!(
        s.describe(&update).unwrap().parameters,
        [DataType::Integer, DataType::Integer]
    );
    assert_eq!(
        s.execute_prepared(&update, &[Value::Int(7), Value::Int(1)], None)
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        result(
            &mut s,
            r#"SELECT "retentionPurgeAt"::text FROM "Interview" WHERE "retentionPurgeAt" = "endedAt" + make_interval(days => 7)"#
        ),
        [[text("2024-01-08 00:00:00+00")]]
    );
    let select = s.prepare("SELECT make_interval(days => $1)").unwrap();
    assert_eq!(s.describe(&select).unwrap().parameters, [DataType::Integer]);
    let both = s
        .prepare("SELECT make_interval(days => $1::int, hours => $2)")
        .unwrap();
    assert_eq!(
        s.describe(&both).unwrap().parameters,
        [DataType::Integer, DataType::Integer]
    );
}

fn failure(s: &mut SqlSession, sql: &str) -> (&'static str, Option<String>, Option<String>) {
    let error = s.execute(sql, &[], None).expect_err(sql);
    (
        sqlstate(&error),
        take_sql_error_message_with_context().0,
        take_sql_error_hint().map(str::to_owned),
    )
}

#[test]
fn unsupported_named_routines_keep_positional_errors() {
    let mut s = session();
    for (named, positional, code) in [
        (
            r#"SELECT jsonb_path_exists(target => '{"a":1}', path => '$.a')"#,
            r#"SELECT jsonb_path_exists('{"a":1}', '$.a')"#,
            "0A000",
        ),
        (
            "SELECT random_normal(mean => 0, stddev => 0)",
            "SELECT random_normal(0, 0)",
            "0A000",
        ),
        (
            "SELECT parse_ident(str => 'a.b')",
            "SELECT parse_ident('a.b')",
            "0A000",
        ),
        (
            r#"SELECT array_sort("array" => ARRAY[2,1], descending => true)"#,
            "SELECT array_sort(ARRAY[2,1], true)",
            "0A000",
        ),
        (
            r#"SELECT jsonb_extract_path('{"a":1}', VARIADIC path_elems => ARRAY['a'])"#,
            r#"SELECT jsonb_extract_path('{"a":1}', ARRAY['a'])"#,
            "0A000",
        ),
        (
            r#"SELECT jsonb_object_field(from_json => '{"a":1}', field_name => 'a')"#,
            r#"SELECT jsonb_object_field('{"a":1}', 'a')"#,
            "0A000",
        ),
        (
            r#"SELECT jsonb_strip_nulls(target => '{"a":null,"b":1}')"#,
            r#"SELECT jsonb_strip_nulls('{"a":null,"b":1}')"#,
            "0A000",
        ),
        (
            r#"SELECT jsonb_insert(jsonb_in => '{"a":[1]}', path => '{a,0}', replacement => '0')"#,
            r#"SELECT jsonb_insert('{"a":[1]}', '{a,0}', '0')"#,
            "0A000",
        ),
        (
            r#"SELECT jsonb_set_lax(jsonb_in => '{"a":1}', path => '{a}', replacement => NULL)"#,
            r#"SELECT jsonb_set_lax('{"a":1}', '{a}', NULL)"#,
            "0A000",
        ),
        (
            r#"SELECT jsonb_extract_path_text(from_json => '{"a":1}', VARIADIC path_elems => ARRAY['a'])"#,
            r#"SELECT jsonb_extract_path_text('{"a":1}', 'a')"#,
            "0A000",
        ),
        (
            r#"SELECT json_extract_path(from_json => '{"a":1}', VARIADIC path_elems => ARRAY['a'])"#,
            r#"SELECT json_extract_path('{"a":1}', 'a')"#,
            "0A000",
        ),
        (
            "SELECT jsonb_array_element(from_json => '[1]', element_index => 0)",
            "SELECT jsonb_array_element('[1]', 0)",
            "0A000",
        ),
        (
            r#"SELECT jsonb_path_match(target => '{"a":1}', path => '$.a == 1')"#,
            r#"SELECT jsonb_path_match('{"a":1}', '$.a == 1')"#,
            "0A000",
        ),
        (
            "SELECT pg_input_error_info(value => 'x', type_name => 'int')",
            "SELECT pg_input_error_info('x', 'int')",
            "0A000",
        ),
        (
            "SELECT pg_terminate_backend(pid => -1)",
            "SELECT pg_terminate_backend(-1)",
            "0A000",
        ),
        (
            "SELECT * FROM json_array_elements(from_json => '[1]')",
            "SELECT * FROM json_array_elements('[1]')",
            "42883",
        ),
        (
            "SELECT * FROM ts_stat(query => 'SELECT ''a''::tsvector')",
            "SELECT * FROM ts_stat('SELECT ''a''::tsvector')",
            "42883",
        ),
        (
            "SELECT * FROM make_interval(days => 1)",
            "SELECT * FROM make_interval(1)",
            "42883",
        ),
        (
            r#"SELECT * FROM jsonb_path_query(target => '{"a":1}', path => '$.a')"#,
            r#"SELECT * FROM jsonb_path_query('{"a":1}', '$.a')"#,
            "42883",
        ),
        (
            r#"SELECT * FROM json_each(from_json => '{"a":1}')"#,
            r#"SELECT * FROM json_each('{"a":1}')"#,
            "42883",
        ),
        (
            "SELECT nosuch.make_interval(days => 3)",
            "SELECT nosuch.make_interval(3)",
            "3F000",
        ),
    ] {
        assert_eq!(
            failure(&mut s, positional),
            (code, None, None),
            "{positional}"
        );
        assert_eq!(failure(&mut s, named), (code, None, None), "{named}");
    }
}

#[test]
fn ambiguous_calls_keep_labels_and_oracle_hint() {
    let mut s = session();
    let message = "function random(min => unknown, max => unknown) is not unique";
    error(
        &mut s,
        "SELECT random(min => 'a', max => 'b')",
        "42725",
        message,
    );
    let prepared = s.prepare("SELECT random(min => $1, max => $2)").unwrap();
    let error = s.describe(&prepared).unwrap_err();
    assert_eq!(sqlstate(&error), "42725");
    assert_eq!(
        take_sql_error_message_with_context().0.as_deref(),
        Some(message)
    );
    assert_eq!(take_sql_error_hint(), Some(AMBIGUOUS_HINT));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn named_ddl_refusal_preserves_stored_catalog_bytes() {
    use rocksdb::{DB, IteratorMode, Options};

    let catalog_bytes = |path: &std::path::Path| {
        let options = Options::default();
        let names = DB::list_cf(&options, path).unwrap();
        let db = DB::open_cf_for_read_only(&options, path, names, false).unwrap();
        let catalog = db.cf_handle("catalog").unwrap();
        db.iterator_cf(&catalog, IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let path = common::fresh("named-ddl");
    let admin = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
    result(
        &mut admin.sql_session(),
        "CREATE TABLE t (id int PRIMARY KEY, d interval, n int DEFAULT 1)",
    );
    drop(admin);
    let before = catalog_bytes(&path);
    assert!(!before.is_empty());

    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for ddl in [
        "CREATE TABLE named_default (id int PRIMARY KEY, d interval DEFAULT make_interval(days => 2::int))",
        "ALTER TABLE t ALTER COLUMN d SET DEFAULT make_interval(days => 2)",
        "ALTER TABLE t ADD CHECK (d >= make_interval(days => 1))",
        "CREATE TABLE named_generated (id int PRIMARY KEY, g interval GENERATED ALWAYS AS (make_interval(days => id)) STORED)",
        "CREATE INDEX named_expression ON t ((make_interval(days => id)))",
        "CREATE INDEX named_partial ON t (id) WHERE d >= make_interval(days => 1)",
        "CREATE VIEW named_list AS SELECT make_interval(days => id) FROM t",
        "CREATE VIEW named_filter AS SELECT id FROM t WHERE d >= make_interval(days := 1)",
    ] {
        assert_eq!(failure(&mut s, ddl), ("0A000", None, None), "{ddl}");
    }
    drop(s);
    drop(admin);
    assert_eq!(catalog_bytes(&path), before);
    std::fs::remove_dir_all(path).unwrap();
}
