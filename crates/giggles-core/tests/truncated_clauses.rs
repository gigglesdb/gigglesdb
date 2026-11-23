#![cfg(feature = "engine-rocksdb")]

mod common;

use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{SqlResult, Value, take_sql_error_message},
    sqlstate,
};
use std::panic::{AssertUnwindSafe, catch_unwind};

const EOF: &str = "syntax error at end of input";

fn with_table(test: impl FnOnce(&mut SqlSession)) {
    let path = common::fresh("truncated-clauses");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut session = admin.sql_session();
        session
            .execute("CREATE TABLE t(a int, b int)", &[], None)
            .unwrap();
        session
            .execute("INSERT INTO t VALUES (1,2),(3,4)", &[], None)
            .unwrap();
        test(&mut session);
    }
    std::fs::remove_dir_all(path).unwrap();
}

fn run(session: &mut SqlSession, sql: &str) -> Result<SqlResult, giggles_core::CoreError> {
    take_sql_error_message();
    catch_unwind(AssertUnwindSafe(|| session.execute(sql, &[], None)))
        .unwrap_or_else(|_| panic!("parser panicked for {sql}"))
}

fn errors(cases: &[(&str, &str, Option<&str>)]) {
    with_table(|session| {
        for &(sql, state, message) in cases {
            let error = run(session, sql).expect_err(sql);
            assert_eq!(sqlstate(&error), state, "{sql}: {error:?}");
            if let Some(message) = message {
                assert_eq!(take_sql_error_message().as_deref(), Some(message), "{sql}");
            }
        }
    });
}

#[test]
fn invalid_grouping_clauses_match_pg18() {
    errors(&[
        // Known divergence: PostgreSQL echoes the spelling LIMIT.
        (
            "SELECT * FROM t group LIMIT 1",
            "42601",
            Some(r#"syntax error at or near "limit""#),
        ),
        ("SELECT * FROM t group", "42601", Some(EOF)),
        ("SELECT * FROM t group by", "42601", Some(EOF)),
        (
            "SELECT * FROM t group having",
            "42601",
            Some(r#"syntax error at or near "having""#),
        ),
        (
            "SELECT * FROM t group order by a",
            "42601",
            Some(r#"syntax error at or near "order""#),
        ),
        (
            "SELECT * FROM t group by having",
            "42601",
            Some(r#"syntax error at or near "having""#),
        ),
        ("SELECT a FROM t group by rollup(", "42601", Some(EOF)),
        ("SELECT a FROM t group by rollup", "42703", None),
        (
            "SELECT a FROM t group by rollup()",
            "42601",
            Some(r#"syntax error at or near ")""#),
        ),
        ("SELECT a FROM t group by cube(", "42601", Some(EOF)),
        ("SELECT a FROM t group by cube", "42703", None),
        ("SELECT a FROM t group by grouping sets", "42601", Some(EOF)),
        (
            "SELECT a FROM t group by grouping sets (",
            "42601",
            Some(EOF),
        ),
        ("SELECT a FROM t group by grouping", "42703", None),
        (
            "SELECT a FROM t group by grouping sets ()",
            "42601",
            Some(r#"syntax error at or near ")""#),
        ),
        ("SELECT a FROM t group by rollup(a,", "42601", Some(EOF)),
        (
            "SELECT a FROM t group by rollup(a) having",
            "42601",
            Some(EOF),
        ),
        ("SELECT a FROM t group by a having", "42601", Some(EOF)),
        (
            "SELECT a FROM t group limit 1",
            "42601",
            Some(r#"syntax error at or near "limit""#),
        ),
        (
            "SELECT a FROM t group group by a",
            "42601",
            Some(r#"syntax error at or near "group""#),
        ),
        (
            "SELECT a FROM t group by group",
            "42601",
            Some(r#"syntax error at or near "group""#),
        ),
        ("SELECT a FROM t group by a,", "42601", Some(EOF)),
        ("SELECT a FROM t group by ()", "42803", None),
        ("SELECT a FROM t group by distinct", "42601", Some(EOF)),
        ("SELECT a FROM t group by all", "42601", Some(EOF)),
    ]);
}

#[test]
fn other_truncated_clauses_match_pg18() {
    errors(&[
        ("SELECT a FROM t having", "42601", Some(EOF)),
        ("SELECT a FROM t window", "42601", Some(EOF)),
        ("SELECT a FROM t window w", "42601", Some(EOF)),
        ("SELECT a FROM t window w as", "42601", Some(EOF)),
        ("SELECT a FROM t window w as (", "42601", Some(EOF)),
        ("SELECT a FROM t limit", "42601", Some(EOF)),
        ("SELECT a FROM t offset", "42601", Some(EOF)),
        ("SELECT a FROM t fetch", "42601", Some(EOF)),
        ("SELECT a FROM t fetch first", "42601", Some(EOF)),
        ("SELECT a FROM t fetch first 1", "42601", Some(EOF)),
        ("SELECT a FROM t fetch first 1 rows", "42601", Some(EOF)),
        ("SELECT a FROM t order", "42601", Some(EOF)),
        ("SELECT a FROM t order by", "42601", Some(EOF)),
        ("SELECT a FROM t limit 1 offset", "42601", Some(EOF)),
        ("SELECT distinct on", "42601", Some(EOF)),
        ("SELECT distinct on (", "42601", Some(EOF)),
        ("SELECT a FROM t where", "42601", Some(EOF)),
        ("SELECT 1 ORDER BY 1 USING", "42601", Some(EOF)),
    ]);
}

#[test]
fn truncated_clauses_at_a_token_boundary() {
    const SEMICOLON: &str = r#"syntax error at or near ";""#;
    const CLOSE: &str = r#"syntax error at or near ")""#;
    errors(&[
        ("SELECT * FROM t group;", "42601", Some(SEMICOLON)),
        ("SELECT * FROM t group by;", "42601", Some(SEMICOLON)),
        ("SELECT a FROM t limit;", "42601", Some(SEMICOLON)),
        ("SELECT a FROM t offset;", "42601", Some(SEMICOLON)),
        ("SELECT a FROM t order by;", "42601", Some(SEMICOLON)),
        ("SELECT a FROM t having;", "42601", Some(SEMICOLON)),
        ("SELECT a FROM t window;", "42601", Some(SEMICOLON)),
        ("SELECT a FROM t fetch;", "42601", Some(SEMICOLON)),
        (
            "SELECT a FROM t fetch first 1 rows;",
            "42601",
            Some(SEMICOLON),
        ),
        (
            "SELECT a FROM t group by rollup(;",
            "42601",
            Some(SEMICOLON),
        ),
        (
            "SELECT a FROM t group by rollup(a,;",
            "42601",
            Some(SEMICOLON),
        ),
        (
            "SELECT a FROM t group by rollup(a) having;",
            "42601",
            Some(SEMICOLON),
        ),
        ("SELECT a FROM t group by a,;", "42601", Some(SEMICOLON)),
        ("SELECT distinct on;", "42601", Some(SEMICOLON)),
        ("SELECT a FROM t window w as;", "42601", Some(SEMICOLON)),
        (
            "SELECT a FROM t group by distinct;",
            "42601",
            Some(SEMICOLON),
        ),
        ("SELECT a FROM t WHERE ;", "42601", Some(SEMICOLON)),
        (
            "SELECT a FROM t ORDER BY a USING;",
            "42601",
            Some(SEMICOLON),
        ),
        (
            "SELECT * FROM (SELECT a FROM t WHERE) s",
            "42601",
            Some(CLOSE),
        ),
        (
            "SELECT * FROM (SELECT a FROM t ORDER BY) s",
            "42601",
            Some(CLOSE),
        ),
        (
            "SELECT * FROM (SELECT a FROM t GROUP BY) s",
            "42601",
            Some(CLOSE),
        ),
        (
            "SELECT * FROM (SELECT a FROM t LIMIT) s",
            "42601",
            Some(CLOSE),
        ),
        // Known divergence: PostgreSQL echoes the spelling LIMIT and HAVING.
        (
            "SELECT a FROM t GROUP BY a ORDER LIMIT 1",
            "42601",
            Some(r#"syntax error at or near "limit""#),
        ),
        (
            "SELECT * FROM t GROUP BY HAVING",
            "42601",
            Some(r#"syntax error at or near "having""#),
        ),
        ("SELECT DISTINCT ON (a a FROM t", "42601", None),
    ]);
}

#[test]
fn within_group_with_paging_or_rollup_is_unsupported() {
    errors(&[
        (
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a) FROM t LIMIT 1",
            "0A000",
            None,
        ),
        (
            "SELECT mode() WITHIN GROUP (ORDER BY a) FROM t OFFSET 0",
            "0A000",
            None,
        ),
        (
            "SELECT b, mode() WITHIN GROUP (ORDER BY a) FROM t GROUP BY ROLLUP(b) ORDER BY b",
            "0A000",
            None,
        ),
    ]);
}

#[test]
fn window_alias_with_union_or_rollup_matches_pg18() {
    with_table(|session| {
        for (sql, expected) in [
            (
                "SELECT a AS window FROM t UNION SELECT 7 ORDER BY 1",
                [Some(1), Some(3), Some(7)],
            ),
            (
                "SELECT a AS window FROM t GROUP BY ROLLUP(a) ORDER BY 1",
                [Some(1), Some(3), None],
            ),
        ] {
            assert_eq!(rows(session, sql), expected, "{sql}");
        }
    });
}

fn rows(session: &mut SqlSession, sql: &str) -> Vec<Option<i64>> {
    run(session, sql)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .rows
        .into_iter()
        .map(|row| match row.values.as_slice() {
            [Value::Null] => None,
            [Value::Int(value)] => Some(*value),
            _ => panic!("{sql}: unexpected row {row:?}"),
        })
        .collect()
}

#[test]
fn valid_grouping_followers_match_pg18() {
    with_table(|session| {
        let all = [None, Some(1), Some(3)];
        let sql = "SELECT a FROM t group by rollup(a) window w as ()";
        let mut result = rows(session, sql);
        result.sort_unstable();
        assert_eq!(result, all, "{sql}");

        for (sql, count) in [
            ("SELECT a FROM t group by rollup(a) limit 1", 1),
            ("SELECT a FROM t group by rollup(a) offset 1", 2),
            (
                "SELECT a FROM t group by rollup(a) fetch first 1 rows only",
                1,
            ),
        ] {
            // With no ORDER BY, pagination may select any submultiset of
            // the oracle's full rollup, with the given cardinality.
            let mut result = rows(session, sql);
            assert_eq!(result.len(), count, "{sql}");
            result.sort_unstable();
            result.dedup();
            assert_eq!(result.len(), count, "{sql}");
            assert!(
                result.iter().all(|value| all.contains(value)),
                "{sql}: {result:?}"
            );
        }
        for (sql, expected) in [
            ("SELECT a FROM t group by rollup(a) order by a limit 1", 1),
            ("SELECT 1 ORDER BY 1 USING >", 1),
        ] {
            assert_eq!(rows(session, sql), [Some(expected)], "{sql}");
        }
    });
}
