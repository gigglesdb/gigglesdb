mod common;

use Value::{Bool, Int, Null};
use giggles_core::{
    Admin, Cancellation, CoreError, Limits, SqlSession,
    relational::{
        SqlError, Value, take_sql_error_detail, take_sql_error_hint, take_sql_error_message,
    },
    sqlstate,
};
use std::time::Instant;

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn session() -> SqlSession {
    let admin = Admin::create(common::fresh("sql-regex"), Limits::default()).unwrap();
    admin.sql_session()
}

fn run(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?} ({:?})", take_sql_error_message()))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}

fn corpus(sql: &str, expected: Vec<Vec<Vec<Value>>>) {
    let mut session = session();
    let sql = sql
        .lines()
        .filter(|line| !line.starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let statements = sql.split(';').map(str::trim).filter(|s| !s.is_empty());
    let mut expected = expected.into_iter();
    for sql in statements {
        let rows = run(&mut session, sql);
        let wanted = if sql.starts_with("SELECT") {
            expected.next().unwrap()
        } else {
            vec![]
        };
        assert_eq!(rows, wanted, "{sql}");
    }
    assert_eq!(expected.next(), None);
}

fn indexed(values: [Value; 3]) -> Vec<Vec<Value>> {
    values
        .into_iter()
        .zip(1..)
        .map(|(val, id)| vec![Int(id), val])
        .collect()
}

#[test]
fn string_patterns() {
    let tf = indexed([Bool(true), Bool(false), Null]);
    let tt = indexed([Bool(true), Bool(true), Null]);
    corpus(
        include_str!("../../../compat/corpus/pg_functions/pgf_string_patterns.sql"),
        vec![
            tf.clone(),
            tf.clone(),
            tt.clone(),
            tt.clone(),
            tf.clone(),
            tf,
            tt,
            indexed([Bool(false), Bool(true), Null]),
            indexed([Bool(true), Bool(true), Bool(true)]),
        ],
    );
}

#[test]
fn string_regex_scalar() {
    corpus(
        include_str!("../../../compat/corpus/pg_functions/pgf_string_regex_scalar.sql"),
        vec![
            indexed([text("{12}"), Null, Null]),
            indexed([text("aXbX"), text(""), Null]),
            indexed([Int(2), Int(0), Null]),
            indexed([Int(2), Int(0), Null]),
            indexed([Bool(true), Bool(false), Null]),
            indexed([text("34"), Null, Null]),
            indexed([text(r#"{a,b,""}"#), text(r#"{""}"#), Null]),
            indexed([text("123-abc"), text("123-abc"), text("123-abc")]),
        ],
    );
}

#[test]
fn string_regex_sets() {
    corpus(
        include_str!("../../../compat/corpus/pg_functions/pgf_string_regex_sets.sql"),
        vec![
            vec![
                vec![Int(1), text("{12}"), Int(1)],
                vec![Int(1), text("{34}"), Int(2)],
            ],
            vec![
                vec![Int(1), text("a"), Int(1)],
                vec![Int(1), text("b"), Int(2)],
                vec![Int(1), text(""), Int(3)],
                vec![Int(2), text(""), Int(1)],
            ],
            vec![vec![text("{ab,12}"), Int(1)], vec![text("{cd,34}"), Int(2)]],
            vec![
                vec![text("a"), Int(1)],
                vec![text("b"), Int(2)],
                vec![text("c"), Int(3)],
            ],
        ],
    );
}

#[test]
fn string_substrings() {
    corpus(
        include_str!("../../../compat/corpus/pg_functions/pgf_string_substrings.sql"),
        vec![
            indexed([text("beta"), text(""), Null]),
            indexed([text("gamma"), text("abc"), Null]),
            indexed([Int(7), Int(2), Null]),
            indexed([Int(7), Int(2), Null]),
            indexed([text("lph"), text("bc"), Null]),
            indexed([text("lph"), text("bc"), Null]),
            indexed([text("alpha"), text("abc"), Null]),
            indexed([text("beta"), Null, Null]),
            indexed([text("ab"), text("ab"), text("ab")]),
            indexed([text("bcdef"), text("bcdef"), text("bcdef")]),
        ],
    );
}

fn diagnostic(s: &mut SqlSession, sql: &str, state: &str, message: &str, hint: Option<&str>) {
    let error = s.execute(sql, &[], None).unwrap_err();
    assert_eq!(sqlstate(&error), state, "{sql}");
    assert_eq!(take_sql_error_message().as_deref(), Some(message), "{sql}");
    assert_eq!(take_sql_error_detail(), None, "{sql}");
    assert_eq!(take_sql_error_hint(), hint, "{sql}");
}

#[test]
fn diagnostics() {
    let mut s = session();
    diagnostic(
        &mut s,
        "SELECT 'abc' ~ '('",
        "2201B",
        "invalid regular expression: parentheses () not balanced",
        None,
    );
    diagnostic(
        &mut s,
        "SELECT 'abc' ~ 'a{2,1}'",
        "2201B",
        "invalid regular expression: invalid repetition count(s)",
        None,
    );
    diagnostic(
        &mut s,
        "SELECT regexp_match('abc','b','g')",
        "22023",
        r#"regexp_match() does not support the "global" option"#,
        Some("Use the regexp_matches function instead."),
    );
    diagnostic(
        &mut s,
        "SELECT regexp_replace('abc','b','x','z')",
        "22023",
        r#"invalid regular expression option: "z""#,
        None,
    );
    diagnostic(
        &mut s,
        "SELECT regexp_count('aaa','a',0)",
        "22023",
        r#"invalid value for parameter "start": 0"#,
        None,
    );
    diagnostic(
        &mut s,
        "SELECT 'x' SIMILAR TO 'x' ESCAPE 'ab'",
        "22025",
        "invalid escape string",
        Some("Escape string must be empty or one character."),
    );
    diagnostic(
        &mut s,
        "SELECT regexp_replace('aaa','a','x','2')",
        "22023",
        r#"invalid regular expression option: "2""#,
        Some(
            "If you meant to use regexp_replace() with a start parameter, cast the fourth argument to integer explicitly.",
        ),
    );
}

#[test]
fn oracle_examples() {
    let mut s = session();
    for (sql, expected) in [
        (
            r#"SELECT 'Abc' ~ 'a' COLLATE "C", 'Abc' ~* 'a' COLLATE "C""#,
            vec![vec![Bool(false), Bool(true)]],
        ),
        (
            r#"SELECT 'ÉCOLE' ~* 'école', 'ÉCOLE' ~* 'école' COLLATE "C""#,
            vec![vec![Bool(true), Bool(false)]],
        ),
        (
            r"SELECT 'aa' ~ '(a)\1', 'xyz' ~ '\d', 'abab' ~ '^(ab)\1$'",
            vec![vec![Bool(true), Bool(false), Bool(true)]],
        ),
        (
            r#"SELECT substring('foobar' FROM 'o(.)b'), substring('foobar', 'o.b'),
            substring('foobar' FROM '%#"o_b#"%' FOR '#'),
            substring('foobar' SIMILAR '%#"o_b#"%' ESCAPE '#')"#,
            vec![vec![text("o"), text("oob"), text("oob"), text("oob")]],
        ),
        (
            "SELECT 'abc' SIMILAR TO 'a%', 'abc' NOT SIMILAR TO 'a%', 'a_c' SIMILAR TO 'a!_c' ESCAPE '!', 'abc' SIMILAR TO 'a%' ESCAPE NULL",
            vec![vec![Bool(true), Bool(false), Bool(true), Null]],
        ),
        (
            "SELECT similar_to_escape('a%b'), similar_to_escape('a!%b','!'), similar_escape('a%b', NULL)",
            vec![vec![
                text("^(?:a.*b)$"),
                text(r"^(?:a\%b)$"),
                text("^(?:a.*b)$"),
            ]],
        ),
        (
            "SELECT regexp_matches('foo bar','(o)(b)?','g')",
            vec![vec![text("{o,NULL}")], vec![text("{o,NULL}")]],
        ),
        (
            "SELECT regexp_split_to_array('a1b22c','[0-9]+'), regexp_split_to_array(NULL,'x')",
            vec![vec![text("{a,b,c}"), Null]],
        ),
        (
            "SELECT pg_typeof(regexp_match('a','a')), pg_typeof(regexp_count('a','a')), pg_typeof(regexp_instr('a','a'))",
            vec![vec![text("text[]"), text("integer"), text("integer")]],
        ),
        (
            "SELECT regexp_instr('abcabc','b',1,2,1), regexp_substr('abcabc','(b)(c)',1,1,'',2)",
            vec![vec![Int(6), text("c")]],
        ),
        (
            "SELECT * FROM regexp_matches('abc','(x)?b')",
            vec![vec![text("{NULL}")]],
        ),
        ("SELECT 'abc' ~ NULL, NULL ~ 'a'", vec![vec![Null, Null]]),
        (
            "SELECT regexp_replace('aaa','a','x',2), regexp_replace('aaa','a','x',2,0), regexp_replace('aaa','a','x',2,1,'g')",
            vec![vec![text("axa"), text("axx"), text("axa")]],
        ),
        (
            "SELECT regexp_split_to_table('a,b,c', ','), regexp_matches('q','q')",
            vec![
                vec![text("a"), text("{q}")],
                vec![text("b"), Null],
                vec![text("c"), Null],
            ],
        ),
        (
            "SELECT regexp_split_to_table('c,b,a', ',') AS part ORDER BY part LIMIT 1 OFFSET 1",
            vec![vec![text("b")]],
        ),
        (
            "SELECT regexp_match('\u{a0}','(.)'), (regexp_match('\u{a0}','(.)'))[1]",
            vec![vec![text("{\u{a0}}"), text("\u{a0}")]],
        ),
        (
            "SELECT 'a'::char(3) ~ '^a$', 'a'::char(3) ~ '^a  $', 'a'::char(3) SIMILAR TO 'a', 'a'::char(3) SIMILAR TO 'a  ', regexp_like('a'::char(3),'^a$')",
            vec![vec![
                Bool(false),
                Bool(true),
                Bool(false),
                Bool(true),
                Bool(true),
            ]],
        ),
        (
            "SELECT 'x' SIMILAR TO 'x' = true, true = 'x' SIMILAR TO 'x'",
            vec![vec![Bool(true), Bool(true)]],
        ),
        (
            "SELECT DISTINCT regexp_split_to_table('b,a,a', ',') AS part ORDER BY part",
            vec![vec![text("a")], vec![text("b")]],
        ),
        (
            "SELECT a.id, regexp_matches(a.s, '(.)','g') FROM (VALUES (1,'ab'), (2,NULL), (3,'c')) AS a(id,s) ORDER BY id",
            vec![
                vec![Int(1), text("{a}")],
                vec![Int(1), text("{b}")],
                vec![Int(3), text("{c}")],
            ],
        ),
        (
            "SELECT regexp_match('NULL, hello', '(NULL)(,)( hello)')",
            vec![vec![text(r#"{"NULL",","," hello"}"#)]],
        ),
    ] {
        assert_eq!(run(&mut s, sql), expected, "{sql}");
    }
}

#[test]
fn set_projection_rejects_row_locking() {
    let mut s = session();
    run(
        &mut s,
        "CREATE TABLE regex_lock (id INT PRIMARY KEY, body TEXT)",
    );
    diagnostic(
        &mut s,
        "SELECT regexp_matches(body, 'x') FROM regex_lock FOR UPDATE",
        "0A000",
        "FOR UPDATE is not allowed with set-returning functions in the target list",
        None,
    );
}

#[test]
fn cancellation_during_regex() {
    let mut s = session();
    let p = s.prepare("SELECT regexp_count($1, 'a')").unwrap();
    let params = [text(&"a".repeat(60_000))];
    let started = Instant::now();
    assert_eq!(
        s.execute_prepared(&p, &params, None).unwrap().rows[0].values,
        vec![Int(60_000)]
    );
    let runtime = started.elapsed();
    let cancelled = Cancellation::new(Some(Instant::now() + runtime / 2));
    let error = cancelled
        .scope(|| s.execute_prepared(&p, &params, None))
        .unwrap_err();
    assert_eq!(error, CoreError::Sql(SqlError::QueryCanceled));
    assert_eq!(sqlstate(&error), "57014");
    assert_eq!(
        take_sql_error_message().as_deref(),
        Some("canceling statement due to user request")
    );
}
