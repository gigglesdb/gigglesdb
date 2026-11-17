mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError as E, Value as V},
};
fn engines() -> Vec<Engine> {
    [
        Engine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        Engine::V2,
    ]
    .to_vec()
}
fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| {
            panic!(
                "{sql}: {e:?}, {:?}, {:?}",
                giggles_core::relational::take_sql_error_message(),
                giggles_core::relational::take_sql_error_detail()
            )
        })
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn fail(s: &mut SqlSession, sql: &str, error: E, message: &str) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(error)),
        "{sql}"
    );
    assert_eq!(
        giggles_core::relational::take_sql_error_message().as_deref(),
        Some(message),
        "{sql}"
    );
}

#[test]
fn ordering_and_distinct_fail_before_reading_rows_but_max_is_lazy() {
    for engine in engines() {
        let a = Admin::create_engine(
            common::fresh("agg-collation-planning"),
            Limits::default(),
            engine,
        )
        .unwrap();
        let mut s = a.sql_session();
        run(
            &mut s,
            r#"CREATE TABLE c1(id int PRIMARY KEY,a text COLLATE "und-x-icu",b text COLLATE "C")"#,
        );
        assert_eq!(
            s.execute("SELECT public.array_agg(DISTINCT a||b) FROM c1", &[], None),
            Err(CoreError::Sql(E::UndefinedFunction))
        );
        for count in 0..=2 {
            for sql in [
                "SELECT string_agg(a, ',' ORDER BY a||b) FROM c1",
                "SELECT count(DISTINCT a||b) FROM c1",
                "SELECT array_agg(DISTINCT a||b) FROM c1",
                "SELECT count(a ORDER BY a||b) FROM c1",
                "SELECT count(DISTINCT a||b) FROM c1 GROUP BY id",
            ] {
                fail(
                    &mut s,
                    sql,
                    E::CollationMismatch,
                    "collation mismatch between implicit collations \"und-x-icu\" and \"C\"",
                );
            }
            if count == 0 {
                assert_eq!(run(&mut s, "SELECT max(a||b) FROM c1"), vec![vec![V::Null]]);
            }
            if count == 1 {
                assert_eq!(
                    run(&mut s, "SELECT max(a||b) FROM c1"),
                    vec![vec![V::Text("ab".into())]]
                );
            }
            if count == 2 {
                fail(
                    &mut s,
                    "SELECT max(a||b) FROM c1",
                    E::IndeterminateCollation,
                    "could not determine which collation to use for string comparison",
                );
            }
            if count < 2 {
                run(
                    &mut s,
                    &format!("INSERT INTO c1 VALUES({},'a','b')", count + 1),
                );
            }
        }
    }
}
