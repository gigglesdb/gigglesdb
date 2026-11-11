mod common;
use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{SqlError, Value as V},
};
fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn exercise(engine: Engine) {
    let a = Admin::create_engine(common::fresh("distinct"), Limits::default(), engine).unwrap();
    let mut s = a.sql_session();
    rows(&mut s, "CREATE TABLE d(a numeric,b int)");
    rows(
        &mut s,
        "INSERT INTO d VALUES(NULL,1),(NULL,2),(1.0,3),(1.00,4),(2,5),('NaN',6),('NaN',7)",
    );
    let ascending = rows(&mut s, "SELECT DISTINCT a FROM d ORDER BY a");
    let expected = ["1.0", "2", "NaN"].map(|n| {
        vec![V::Numeric(
            giggles_core::relational::Numeric::parse(n).unwrap(),
        )]
    });
    assert_eq!(ascending[..3], expected);
    assert_eq!(ascending[3], vec![V::Null]);
    for sql in [
        "SELECT DISTINCT a FROM d ORDER BY 1",
        "SELECT DISTINCT a AS n FROM d ORDER BY n",
        "SELECT DISTINCT d.a FROM d ORDER BY a",
    ] {
        assert_eq!(rows(&mut s, sql), ascending);
    }
    let mut descending = rows(
        &mut s,
        "SELECT DISTINCT a FROM d ORDER BY a DESC NULLS FIRST",
    );
    descending.reverse();
    assert_eq!(descending, ascending);
    assert_eq!(
        rows(
            &mut s,
            "SELECT DISTINCT a FROM d ORDER BY a LIMIT 2 OFFSET 1"
        ),
        ascending[1..3]
    );
    for (sql, count) in [
        ("SELECT DISTINCT a,a+1 AS n FROM d ORDER BY n", 4),
        ("SELECT DISTINCT * FROM d ORDER BY 2", 7),
        ("SELECT DISTINCT a,b FROM d ORDER BY a,b", 7),
    ] {
        assert_eq!(rows(&mut s, sql).len(), count);
    }
    for sql in [
        "SELECT DISTINCT count(*) FROM d GROUP BY b",
        "SELECT DISTINCT count(*) AS n FROM d GROUP BY b ORDER BY n",
    ] {
        assert_eq!(rows(&mut s, sql), vec![vec![V::Int(1)]]);
    }
    for sql in [
        "SELECT DISTINCT a FROM d ORDER BY b",
        "SELECT DISTINCT a FROM d ORDER BY a+1",
        "SELECT DISTINCT count(*) FROM d GROUP BY b ORDER BY b",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(giggles_core::CoreError::Sql(SqlError::GroupingError)),
            "{sql}"
        );
    }
    rows(&mut s, "CREATE TABLE target(a numeric)");
    rows(&mut s, "INSERT INTO target SELECT DISTINCT a FROM d");
    assert_eq!(rows(&mut s, "SELECT a FROM target ORDER BY a"), ascending);
    assert_eq!(
        rows(
            &mut s,
            "SELECT b FROM d WHERE a IN (SELECT DISTINCT a FROM d) ORDER BY b"
        )
        .len(),
        5
    );
}
#[test]
fn legacy_pg18_distinct() {
    exercise(Engine::Legacy);
}
#[test]
#[cfg(feature = "engine-rocksdb")]
fn v2_pg18_distinct() {
    exercise(Engine::V2);
}
