mod common;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, Value as V},
};
fn session() -> (Admin, SqlSession) {
    let a = Admin::create(common::fresh("expression-collation"), Limits::default()).unwrap();
    let s = a.sql_session();
    (a, s)
}
fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn text(v: &str) -> V {
    V::Text(v.into())
}
fn fixture(s: &mut SqlSession) {
    rows(s, "CREATE TABLE ct(id int PRIMARY KEY,v text,w text)");
    rows(
        s,
        "INSERT INTO ct VALUES(1,'z','a'),(2,'a','B'),(3,'?','b'),(4,'Z','A'),(5,'A',NULL)",
    );
}
#[test]
fn expressions_comparisons_and_errors() {
    assert!(bincode::serialize(&SqlError::CollationMismatch).is_err());
    for (error, tag) in [
        (SqlError::InvalidRecursion, 58),
        (SqlError::DuplicateAlias, 59),
        (SqlError::CollationMismatch, 64),
        (SqlError::IndeterminateCollation, 65),
    ] {
        assert_eq!(giggles_core::local::failure(CoreError::Sql(error)).code, 200 + tag);
    }
    assert_eq!(
        giggles_core::sqlstate(&CoreError::Sql(SqlError::CollationMismatch)),
        "42P21"
    );
    let (_a, mut s) = session();
    for (sql, result) in [
        (
            r#"SELECT '?' COLLATE "und-x-icu" < 'z' COLLATE "und-x-icu""#,
            V::Bool(true),
        ),
        (r#"SELECT 'a' COLLATE "C" < 'B'"#, V::Bool(false)),
        (r#"SELECT 'a' COLLATE "en_US""#, text("a")),
        (r#"SELECT lower('I' COLLATE "tr-x-icu")"#, text("ı")),
        (r#"SELECT upper('i' COLLATE "tr-x-icu")"#, text("İ")),
        (r#"SELECT upper('ä' COLLATE "tr-x-icu")"#, text("Ä")),
        (r#"SELECT upper('ß' COLLATE "pg_unicode_fast")"#, text("SS")),
        (r#"SELECT upper('ß' COLLATE "pg_c_utf8")"#, text("ß")),
        (r#"SELECT upper('ß' COLLATE "C")"#, text("ß")),
        (
            r#"SELECT pg_collation_for('x' COLLATE pg_catalog."und-x-icu")"#,
            text("\"und-x-icu\""),
        ),
        ("SELECT pg_collation_for('x')", V::Null),
    ] {
        assert_eq!(rows(&mut s, sql), vec![vec![result]], "{sql}");
    }
    rows(&mut s, "CREATE TYPE coll_enum AS ENUM ('a','b')");
    rows(
        &mut s,
        "CREATE TABLE enum_values(id int PRIMARY KEY,e coll_enum)",
    );
    rows(&mut s, "INSERT INTO enum_values VALUES(1,'a')");
    assert_eq!(
        s.execute(r#"SELECT e COLLATE "C" FROM enum_values"#, &[], None)
            .unwrap_err(),
        CoreError::Sql(SqlError::Type)
    );
    for (sql, error) in [
        (
            r#"SELECT 'a' COLLATE "UND-X-ICU""#,
            SqlError::UndefinedObject,
        ),
        (r#"SELECT 'a' COLLATE "missing""#, SqlError::UndefinedObject),
        (r#"SELECT 1 COLLATE "C""#, SqlError::Type),
        (
            r#"SELECT 'a' COLLATE "C" < 'B' COLLATE "und-x-icu""#,
            SqlError::CollationMismatch,
        ),
        (
            r#"SELECT 'a' COLLATE "de-x-icu" || 'b' COLLATE "C""#,
            SqlError::CollationMismatch,
        ),
        (r#"SELECT 'a' COLLATE "en_US" < 'b'"#, SqlError::Unsupported),
        (
            r#"SELECT lower('I' COLLATE "en_US")"#,
            SqlError::Unsupported,
        ),
    ] {
        assert_eq!(
            s.execute(sql, &[], None).unwrap_err(),
            CoreError::Sql(error),
            "{sql}"
        );
    }
}
#[test]
fn ordering_aggregates_distinct_and_groups() {
    let (_a, mut s) = session();
    fixture(&mut s);
    let expected = ["?", "a", "A", "z", "Z"].map(|v| vec![text(v)]).to_vec();
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT v FROM ct ORDER BY v COLLATE "und-x-icu",v COLLATE "C""#
        ),
        expected
    );
    for n in [
        "ucs_basic",
        "pg_unicode_fast",
        "C",
        "POSIX",
        "C.utf8",
        "pg_c_utf8",
        "default",
    ] {
        assert_eq!(
            rows(
                &mut s,
                &format!("SELECT v FROM ct ORDER BY v COLLATE \"{n}\",id")
            ),
            ["?", "A", "Z", "a", "z"].map(|v| vec![text(v)])
        );
    }
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT v < w COLLATE "und-x-icu" FROM ct ORDER BY id"#
        ),
        [Some(false), Some(true), Some(true), Some(false), None]
            .map(|v| vec![v.map_or(V::Null, V::Bool)])
    );
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT w FROM ct ORDER BY w COLLATE "und-x-icu" DESC NULLS FIRST"#
        ),
        vec![
            vec![V::Null],
            vec![text("B")],
            vec![text("b")],
            vec![text("A")],
            vec![text("a")]
        ]
    );
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT min(v COLLATE "und-x-icu"),max(v COLLATE "und-x-icu") FROM ct"#
        ),
        vec![vec![text("?"), text("Z")]]
    );
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT greatest('a' COLLATE "und-x-icu",'B'),least('a','B' COLLATE "C")"#
        ),
        vec![vec![text("B"), text("B")]]
    );
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT v FROM ct WHERE v COLLATE "und-x-icu" BETWEEN 'a' AND 'b' ORDER BY v COLLATE "und-x-icu""#
        ),
        vec![vec![text("a")], vec![text("A")]]
    );
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT DISTINCT v COLLATE "und-x-icu" FROM ct ORDER BY 1"#
        ),
        expected
    );
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT v COLLATE "und-x-icu" FROM ct GROUP BY v COLLATE "und-x-icu" ORDER BY 1"#
        ),
        expected
    );
    assert_eq!(
        rows(&mut s, "SELECT pg_collation_for(v) FROM ct WHERE id=1"),
        vec![vec![text("\"default\"")]]
    );
}

#[test]
fn sql_layer_matches_postgres_700_string_orders() {
    let (_a, mut s) = session();
    rows(&mut s, "CREATE TABLE words(id int PRIMARY KEY,v text)");
    let oracle = include_str!("fixtures/expression_collation_orders.txt");
    let values: Vec<_> = oracle
        .lines()
        .filter_map(|l| l.strip_prefix("S|"))
        .map(|l| {
            let (id, v) = l.split_once('|').unwrap();
            format!("({id},'{}')", v.replace("'", "''"))
        })
        .collect();
    assert_eq!(values.len(), 700);
    for chunk in values.chunks(50) {
        rows(
            &mut s,
            &format!("INSERT INTO words VALUES{}", chunk.join(",")),
        );
    }
    let mut icu_count = 0;
    for l in oracle.lines().filter_map(|l| l.strip_prefix("O|")) {
        let (n, ids) = l.split_once('|').unwrap();
        if n.ends_with("-x-icu") || n == "unicode" {
            icu_count += 1;
        }
        let expected: Vec<_> = ids
            .split(',')
            .map(|v| vec![V::Int(v.parse().unwrap())])
            .collect();
        assert_eq!(
            rows(
                &mut s,
                &format!("SELECT id FROM words ORDER BY v COLLATE \"{n}\",id")
            ),
            expected,
            "{n}"
        );
        if n == "C" {
            assert_eq!(
                rows(
                    &mut s,
                    r#"SELECT id FROM words ORDER BY v COLLATE "default",id"#
                ),
                expected,
                "database default"
            );
        }
    }
    assert!(icu_count >= 40);
}

#[test]
fn silent_parity_corpus() {
    let (_a, mut s) = session();
    let corpus = include_str!("../../../compat/corpus/pg_silent/pgs_collation.sql");
    let results: Vec<_> = corpus
        .split(';')
        .filter(|sql| !sql.trim().is_empty())
        .map(|sql| rows(&mut s, sql))
        .collect();
    assert_eq!(results[2], ["?", "a", "A", "z", "Z"].map(|v| vec![text(v)]));
    assert_eq!(results[3], vec![vec![V::Bool(true)]]);
}
