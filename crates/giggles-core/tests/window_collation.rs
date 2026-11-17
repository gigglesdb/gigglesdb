#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError as E, Value as V},
};

fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| {
            panic!(
                "{sql}: {e:?}, {:?}",
                giggles_core::relational::take_sql_error_message()
            )
        })
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn fixture() -> (Admin, SqlSession) {
    let a = Admin::create_engine(
        common::fresh("window-collation"),
        Limits::default(),
        Engine::V2,
    )
    .unwrap();
    let mut s = a.sql_session();
    for sql in [
        "CREATE COLLATION ci (provider=icu,locale='und-u-ks-level2',deterministic=false)",
        "CREATE COLLATION cd (provider=icu,locale='und-u-ks-level2',deterministic=true)",
        r#"CREATE TABLE w(id int PRIMARY KEY,v text COLLATE "und-x-icu",c text COLLATE ci,t text,u text COLLATE "C",d text COLLATE cd)"#,
        "INSERT INTO w VALUES(1,'b','b','b','b','b'),(2,'B','B','B','B','B'),(3,'a','a','a','a','a'),(4,'Z','Z','Z','Z','Z'),(5,'b','b','b','b','b'),(6,NULL,NULL,NULL,NULL,NULL)",
    ] {
        run(&mut s, sql);
    }
    (a, s)
}
fn ints<const N: usize>(values: &[[i64; N]]) -> Vec<Vec<V>> {
    values
        .iter()
        .map(|r| r.iter().copied().map(V::Int).collect())
        .collect()
}
fn text(s: &str) -> V {
    V::Text(s.into())
}

// Expected rows and SQLSTATEs were captured from PostgreSQL 18.6 / ICU 76.1.
// row_number's tie order is fixed with id; rank/frames retain collation peers.
#[test]
fn deterministic_icu_order_partition_and_range_peers() {
    let (_a, mut s) = fixture();
    assert_eq!(
        run(
            &mut s,
            "SELECT id,row_number() OVER (ORDER BY v,id),rank() OVER (ORDER BY v),dense_rank() OVER (ORDER BY v),count(*) OVER (ORDER BY v RANGE BETWEEN CURRENT ROW AND CURRENT ROW),count(*) OVER (PARTITION BY v) FROM w ORDER BY id"
        ),
        ints(&[
            [1, 2, 2, 2, 2, 2],
            [2, 4, 4, 3, 1, 1],
            [3, 1, 1, 1, 1, 1],
            [4, 5, 5, 4, 1, 1],
            [5, 3, 2, 2, 2, 2],
            [6, 6, 6, 5, 1, 1]
        ])
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT v,row_number() OVER (ORDER BY v,id) FROM w ORDER BY 2"
        ),
        vec![
            vec![text("a"), V::Int(1)],
            vec![text("b"), V::Int(2)],
            vec![text("b"), V::Int(3)],
            vec![text("B"), V::Int(4)],
            vec![text("Z"), V::Int(5)],
            vec![V::Null, V::Int(6)]
        ]
    );
}
#[test]
fn nondeterministic_partition_rank_and_range_peers() {
    let (_a, mut s) = fixture();
    assert_eq!(
        run(
            &mut s,
            "SELECT id,row_number() OVER (ORDER BY c,id),rank() OVER (ORDER BY c),dense_rank() OVER (ORDER BY c),count(*) OVER (ORDER BY c RANGE BETWEEN CURRENT ROW AND CURRENT ROW),count(*) OVER (PARTITION BY c) FROM w ORDER BY id"
        ),
        ints(&[
            [1, 2, 2, 2, 3, 3],
            [2, 3, 2, 2, 3, 3],
            [3, 1, 1, 1, 1, 1],
            [4, 5, 5, 3, 1, 1],
            [5, 4, 2, 2, 3, 3],
            [6, 6, 6, 4, 1, 1]
        ])
    );
}
#[test]
fn explicit_collate_groups_and_deterministic_byte_tiebreak() {
    let (_a, mut s) = fixture();
    assert_eq!(
        run(
            &mut s,
            r#"SELECT id,row_number() OVER (ORDER BY t COLLATE "und-x-icu",id),count(*) OVER (PARTITION BY t COLLATE ci),count(*) OVER (ORDER BY t COLLATE ci GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW),count(*) OVER (PARTITION BY d),rank() OVER (ORDER BY d) FROM w ORDER BY id"#
        ),
        ints(&[
            [1, 2, 3, 4, 2, 3],
            [2, 4, 3, 4, 1, 2],
            [3, 1, 1, 1, 1, 1],
            [4, 5, 1, 4, 1, 5],
            [5, 3, 3, 4, 2, 3],
            [6, 6, 1, 2, 1, 6]
        ])
    );
    assert_eq!(
        run(
            &mut s,
            r#"SELECT id,row_number() OVER (ORDER BY v COLLATE "C",id),count(*) OVER (PARTITION BY c COLLATE "C") FROM w ORDER BY id"#
        ),
        ints(&[
            [1, 4, 2],
            [2, 1, 1],
            [3, 3, 1],
            [4, 2, 1],
            [5, 5, 2],
            [6, 6, 1]
        ])
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id,row_number() OVER (ORDER BY v DESC NULLS FIRST,id),row_number() OVER (ORDER BY c DESC NULLS LAST,id) FROM w ORDER BY id"
        ),
        ints(&[
            [1, 4, 2],
            [2, 3, 3],
            [3, 6, 5],
            [4, 2, 1],
            [5, 5, 4],
            [6, 1, 6]
        ])
    );
}
#[test]
fn aggregate_and_value_windows_use_icu_ordering() {
    let (_a, mut s) = fixture();
    assert_eq!(
        run(
            &mut s,
            "SELECT min(v) OVER (),max(v) OVER (),first_value(v) OVER (ORDER BY v),last_value(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),nth_value(v,2) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM w ORDER BY id"
        ),
        vec![vec![text("a"), text("Z"), text("a"), V::Null, text("b")]; 6]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT first_value(c) OVER (ORDER BY c,id),last_value(c) OVER (ORDER BY c,id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),nth_value(c,3) OVER (ORDER BY c,id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM w ORDER BY id"
        ),
        vec![vec![text("a"), V::Null, text("B")]; 6]
    );
}
#[test]
fn exclusions_and_named_windows_keep_nondeterministic_peers() {
    let (_a, mut s) = fixture();
    assert_eq!(
        run(
            &mut s,
            "SELECT id,count(*) OVER (ORDER BY c RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES),count(*) OVER (ORDER BY c GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING EXCLUDE GROUP) FROM w ORDER BY id"
        ),
        ints(&[
            [1, 2, 1],
            [2, 2, 1],
            [3, 1, 3],
            [4, 5, 1],
            [5, 2, 1],
            [6, 6, 0]
        ])
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT count(*) OVER win FROM w WINDOW win AS (PARTITION BY c) ORDER BY id"
        ),
        ints(&[[3], [3], [1], [1], [3], [1]])
    );
}
#[test]
fn derivation_errors_overrides_and_noncomparison_results() {
    let (_a, mut s) = fixture();
    for (sql, error) in [
        (
            "SELECT row_number() OVER (ORDER BY v||u) FROM w",
            E::CollationMismatch,
        ),
        (
            "SELECT row_number() OVER (PARTITION BY v||u) FROM w",
            E::CollationMismatch,
        ),
        (
            r#"SELECT row_number() OVER (ORDER BY v COLLATE "C"||u COLLATE "und-x-icu") FROM w"#,
            E::CollationMismatch,
        ),
        ("SELECT min(v||u) OVER () FROM w", E::IndeterminateCollation),
        ("SELECT max(v||u) OVER () FROM w", E::IndeterminateCollation),
    ] {
        assert_eq!(
            s.execute(sql, &[], None).unwrap_err(),
            CoreError::Sql(error),
            "{sql}"
        );
    }
    assert_eq!(
        run(
            &mut s,
            r#"SELECT id,row_number() OVER (ORDER BY (v||u) COLLATE "und-x-icu",id) FROM w ORDER BY id"#
        ),
        ints(&[[1, 2], [2, 4], [3, 1], [4, 5], [5, 3], [6, 6]])
    );
    assert_eq!(
        run(&mut s, "SELECT first_value(v||u) OVER (ORDER BY id) FROM w"),
        vec![vec![text("bb")]; 6]
    );
    assert_eq!(
        run(&mut s, "SELECT min(v||u) OVER () FROM w WHERE id=1"),
        vec![vec![text("bb")]]
    );
}
#[test]
fn projected_window_values_retain_collation() {
    let (_a, mut s) = fixture();
    assert_eq!(
        run(
            &mut s,
            "SELECT lag(v,0) OVER (ORDER BY id) AS x FROM w ORDER BY x"
        ),
        [
            text("a"),
            text("b"),
            text("b"),
            text("B"),
            text("Z"),
            V::Null
        ]
        .map(|x| vec![x])
        .to_vec()
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT DISTINCT lag(c,0) OVER (ORDER BY id) AS x FROM w"
        )
        .len(),
        4
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT c,count(c),rank() OVER (ORDER BY count(c)) FROM w GROUP BY c ORDER BY 3,c"
        ),
        vec![
            vec![V::Null, V::Int(0), V::Int(1)],
            vec![text("a"), V::Int(1), V::Int(2)],
            vec![text("Z"), V::Int(1), V::Int(2)],
            vec![text("b"), V::Int(3), V::Int(4)]
        ]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT v,rank() OVER (ORDER BY v) FROM w GROUP BY v ORDER BY 2"
        ),
        vec![
            vec![text("a"), V::Int(1)],
            vec![text("b"), V::Int(2)],
            vec![text("B"), V::Int(3)],
            vec![text("Z"), V::Int(4)],
            vec![V::Null, V::Int(5)]
        ]
    );
}
#[test]
fn other_new_planner_paths_fail_closed_for_collated_inputs() {
    let (_a, mut s) = fixture();
    for sql in [
        "SELECT x.id FROM w x RIGHT JOIN w y ON x.v=y.v",
        "SELECT x.id FROM w x FULL JOIN w y ON x.c=y.c",
        "SELECT x.id FROM w x CROSS JOIN w y",
        "SELECT x.id FROM w x,w y",
        "SELECT id FROM w x NATURAL JOIN w y",
        "SELECT id FROM w x JOIN w y USING(id)",
        "SELECT (SELECT x.v FROM w x WHERE x.id=w.id) FROM w",
        "SELECT x.id FROM w x,LATERAL (SELECT x.c) y",
        "SELECT w.id FROM w,generate_series(1,2) x",
        "SELECT count(*) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::Unsupported)),
            "{sql}"
        );
    }
}

#[test]
fn legacy_subquery_routes_keep_collation_or_fail_closed() {
    let (_a, mut s) = fixture();
    for sql in [
        "SELECT id FROM w WHERE c=ANY(SELECT c FROM w WHERE id=2) ORDER BY id",
        "SELECT id FROM w WHERE c=ALL(SELECT c FROM w WHERE id=2) ORDER BY id",
        "SELECT id FROM w WHERE EXISTS(SELECT 1 FROM w x WHERE x.c=w.c AND x.id=2) ORDER BY id",
    ] {
        match s.execute(sql, &[], None) {
            Ok(result) => assert_eq!(
                result
                    .rows
                    .into_iter()
                    .map(|r| r.values)
                    .collect::<Vec<_>>(),
                ints(&[[1], [2], [5]]),
                "{sql}"
            ),
            Err(error) => assert_eq!(error, CoreError::Sql(E::Unsupported), "{sql}"),
        }
    }
    let sql = "SELECT c,count(*) FROM w GROUP BY c HAVING EXISTS(SELECT 1 FROM w x WHERE x.c=w.c AND x.id=2)";
    match s.execute(sql, &[], None) {
        Ok(result) => assert_eq!(
            result
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            vec![vec![text("b"), V::Int(3)]],
            "{sql}"
        ),
        Err(error) => assert_eq!(error, CoreError::Sql(E::Unsupported), "{sql}"),
    }
}

#[test]
fn simple_derived_input_preserves_column_collations() {
    let (_a, mut s) = fixture();
    assert_eq!(
        run(
            &mut s,
            "SELECT id,rank() OVER (ORDER BY v),count(*) OVER (PARTITION BY c) FROM (SELECT id,v,c FROM w) x ORDER BY id"
        ),
        ints(&[
            [1, 2, 3],
            [2, 4, 3],
            [3, 1, 1],
            [4, 5, 1],
            [5, 2, 3],
            [6, 6, 1]
        ])
    );
}

#[test]
fn parenthesized_window_result_keeps_collation_for_outer_sort() {
    let (_a, mut s) = fixture();
    assert_eq!(
        run(
            &mut s,
            "(SELECT lag(v,0) OVER (ORDER BY id) AS x FROM w) ORDER BY 1 LIMIT 5"
        ),
        ["a", "b", "b", "B", "Z"].map(|x| vec![text(x)]).to_vec()
    );
}
