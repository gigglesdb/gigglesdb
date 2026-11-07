mod common;
use common::*;
use giggles_core::{
    relational::{DataType, Numeric, SqlError as E, Value as V},
    *,
};
use std::cmp::Ordering;

fn db() -> (Admin, SqlSession) {
    let a = Admin::create(fresh("relational-scans"), Limits::default()).unwrap();
    let s = a.sql_session();
    (a, s)
}
fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .expect(sql)
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn ids(xs: impl IntoIterator<Item = i64>) -> Vec<Vec<V>> {
    xs.into_iter().map(|x| vec![V::Int(x)]).collect()
}
fn fixture(s: &mut SqlSession, n: i64) {
    run(
        s,
        "CREATE TABLE scan_rows (id BIGINT PRIMARY KEY, score BIGINT, bucket BIGINT)",
    );
    run(s, "BEGIN");
    let p = s.prepare("INSERT INTO scan_rows VALUES($1,$2,$3)").unwrap();
    for i in (0..n).rev() {
        let score = if i % 4 == 0 { V::Null } else { V::Int(i % 7) };
        s.execute_prepared(&p, &[V::Int(i), score, V::Int(i % 3)], None)
            .unwrap();
    }
    run(s, "COMMIT");
}
fn score(i: i64) -> Option<i64> {
    (i % 4 != 0).then_some(i % 7)
}

#[test]
fn unbounded_scan_and_non_key_predicates_match_rust() {
    let (_a, mut s) = db();
    fixture(&mut s, 19);
    let expected: Vec<_> = (0..19)
        .map(|i| vec![V::Int(i), score(i).map_or(V::Null, V::Int), V::Int(i % 3)])
        .collect();
    assert_eq!(run(&mut s, "SELECT * FROM scan_rows"), expected);
    type Predicate = (&'static str, fn(i64) -> bool);
    let predicates: [Predicate; 8] = [
        ("score=3", |i| score(i) == Some(3)),
        ("score<3", |i| score(i).is_some_and(|v| v < 3)),
        ("score<=3", |i| score(i).is_some_and(|v| v <= 3)),
        ("score>3", |i| score(i).is_some_and(|v| v > 3)),
        ("score>=3", |i| score(i).is_some_and(|v| v >= 3)),
        ("score>=3 AND bucket=1", |i| {
            score(i).is_some_and(|v| v >= 3) && i % 3 == 1
        }),
        ("score=3 OR bucket=1", |i| score(i) == Some(3) || i % 3 == 1),
        ("score IS NULL", |i| score(i).is_none()),
    ];
    for (sql, predicate) in predicates {
        assert_eq!(
            run(&mut s, &format!("SELECT id FROM scan_rows WHERE {sql}")),
            ids((0..19).filter(|i| predicate(*i))),
            "{sql}"
        );
    }
}

#[test]
fn non_key_sort_null_defaults_mixed_directions_and_every_limit_prefix() {
    let (_a, mut s) = db();
    fixture(&mut s, 19);
    for (order, descending, null_first, mixed) in [
        ("score ASC NULLS FIRST, id ASC", false, true, false),
        ("score ASC NULLS LAST, id ASC", false, false, false),
        ("score DESC NULLS FIRST, id ASC", true, true, false),
        ("score DESC NULLS LAST, id ASC", true, false, false),
        ("score ASC, id ASC", false, false, false),
        ("score DESC, id ASC", true, true, false),
        (
            "score ASC NULLS LAST, bucket DESC, id ASC",
            false,
            false,
            true,
        ),
    ] {
        let mut expected: Vec<i64> = (0..19).collect();
        expected.sort_by(|a, b| {
            let cmp = match (score(*a), score(*b)) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => {
                    if null_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (Some(_), None) => {
                    if null_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (Some(a), Some(b)) => {
                    if descending {
                        b.cmp(&a)
                    } else {
                        a.cmp(&b)
                    }
                }
            };
            cmp.then_with(|| {
                if mixed {
                    (b % 3).cmp(&(a % 3))
                } else {
                    Ordering::Equal
                }
            })
            .then_with(|| a.cmp(b))
        });
        assert_eq!(
            run(
                &mut s,
                &format!("SELECT id FROM scan_rows ORDER BY {order}")
            ),
            ids(expected.iter().copied()),
            "{order}"
        );
        for k in [0, 1, 3, 18, 19, 24] {
            assert_eq!(
                run(
                    &mut s,
                    &format!("SELECT id FROM scan_rows ORDER BY {order} LIMIT {k}")
                ),
                ids(expected.iter().copied().take(k)),
                "{order} LIMIT {k}"
            );
        }
    }
}

#[test]
fn large_scan_limits_aggregates_parameters_explain_and_cancellation() {
    let (_a, mut s) = db();
    fixture(&mut s, 3000);
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM scan_rows WHERE bucket=1 AND abs(id)<15"
        ),
        ids([1, 4, 7, 10, 13])
    );
    assert_eq!(
        run(&mut s, "SELECT id FROM scan_rows LIMIT 2000"),
        ids(0..2000)
    );
    assert_eq!(
        run(&mut s, "SELECT id FROM scan_rows LIMIT NULL"),
        ids(0..3000)
    );
    assert_eq!(
        s.execute("SELECT id FROM scan_rows LIMIT -1", &[], None),
        Err(CoreError::Sql(E::Limit))
    );
    let mut ordered: Vec<i64> = (0..3000).collect();
    ordered.sort_by_key(|i| (score(*i).is_none(), score(*i), *i));
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM scan_rows ORDER BY score, id LIMIT 10"
        ),
        ids(ordered.into_iter().take(10))
    );
    let kept: Vec<_> = (0..3000).filter(|i| i % 3 == 1).collect();
    let present: Vec<_> = kept.iter().filter_map(|i| score(*i)).collect();
    assert_eq!(
        run(
            &mut s,
            "SELECT count(*),count(score),sum(score),min(score),max(score) FROM scan_rows WHERE bucket=1"
        ),
        vec![vec![
            V::Int(kept.len() as i64),
            V::Int(present.len() as i64),
            V::Numeric(Numeric::parse(&present.iter().sum::<i64>().to_string()).unwrap()),
            V::Int(0),
            V::Int(6)
        ]]
    );
    assert!(run(&mut s, "SELECT count(*),count(score),sum(score),min(score),max(score) FROM scan_rows WHERE bucket=1 LIMIT 0").is_empty());
    let p = s
        .prepare("SELECT id FROM scan_rows WHERE bucket=$1 LIMIT $2")
        .unwrap();
    assert_eq!(
        s.describe(&p).unwrap().parameters,
        vec![DataType::BigInt, DataType::BigInt]
    );
    let got: Vec<_> = s
        .execute_prepared(&p, &[V::Int(1), V::Int(5)], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect();
    assert_eq!(got, ids([1, 4, 7, 10, 13]));
    let plan = run(
        &mut s,
        "EXPLAIN SELECT id FROM scan_rows ORDER BY score LIMIT 10",
    );
    assert!(plan.iter().flatten().any(|v| matches!(v,V::Text(p) if p.contains("PrimaryKeyRangeScan") && p.contains("TopNSort"))), "{plan:?}");
    let cancellation = Cancellation::default();
    cancellation.cancel();
    run(&mut s, "BEGIN");
    assert_eq!(
        cancellation.scope(|| s.execute("SELECT id FROM scan_rows WHERE score=6", &[], None)),
        Err(CoreError::Cancelled)
    );
    assert_eq!(
        s.execute("SELECT id FROM scan_rows", &[], None),
        Err(CoreError::Sql(E::Aborted))
    );
    s.rollback();
}

#[test]
fn result_byte_cap_is_enforced() {
    let (_a, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE wide_rows (id BIGINT PRIMARY KEY, body TEXT)",
    );
    run(&mut s, "BEGIN");
    let p = s.prepare("INSERT INTO wide_rows VALUES($1,$2)").unwrap();
    for i in 0..1100 {
        s.execute_prepared(&p, &[V::Int(i), V::Text("x".repeat(1024))], None)
            .unwrap();
    }
    run(&mut s, "COMMIT");
    assert_eq!(
        s.execute("SELECT * FROM wide_rows", &[], None),
        Err(CoreError::Sql(E::Limit))
    );
}

#[test]
fn redundant_and_mixed_key_predicates_fall_back_to_filtered_scan() {
    let (_a, mut s) = db();
    fixture(&mut s, 19);
    for (q, want) in [
        ("SELECT id FROM scan_rows WHERE id > 1 AND id > 15", vec![16, 17, 18]),
        ("SELECT id FROM scan_rows WHERE id >= 1 AND id <= 10 AND id > 7", vec![8, 9, 10]),
        ("SELECT id FROM scan_rows WHERE id = 3 AND id > 1", vec![3]),
        ("SELECT id FROM scan_rows WHERE id = 3 AND id > 5", vec![]),
        ("SELECT id FROM scan_rows WHERE id < 4 AND id < 2", vec![0, 1]),
    ] {
        assert_eq!(run(&mut s, q), ids(want), "{q}");
    }
}

fn sql_and(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn sql_or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn sql_in(value: Option<i64>, candidates: &[Option<i64>]) -> Option<bool> {
    let mut result = Some(false);
    for candidate in candidates {
        result = sql_or(result, value.zip(*candidate).map(|(a, b)| a == b));
    }
    result
}

#[test]
fn membership_and_inequality_r0_queries() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE t (id integer PRIMARY KEY, name text)");
    for id in 1..=3 {
        run(&mut s, &format!("INSERT INTO t VALUES ({id}, 'row{id}')"));
    }
    assert_eq!(
        run(&mut s, "SELECT id FROM t WHERE id IN (1, 3) ORDER BY id"),
        ids((1..=3).filter(|id| sql_in(Some(*id), &[Some(1), Some(3)]) == Some(true)))
    );
    assert_eq!(
        run(&mut s, "SELECT id FROM t WHERE id <> 1 ORDER BY id"),
        ids((1..=3).filter(|id| Some(*id != 1) == Some(true)))
    );
}

#[test]
fn inequality_predicates_match_sql_three_value_logic() {
    let (_a, mut s) = db();
    fixture(&mut s, 29);
    type Predicate = (&'static str, fn(i64) -> Option<bool>);
    let predicates: [Predicate; 8] = [
        ("id <> 1", |i| Some(i != 1)),
        ("id != 1", |i| Some(i != 1)),
        ("score <> 1", |i| score(i).map(|v| v != 1)),
        ("score != 1", |i| score(i).map(|v| v != 1)),
        ("id <> NULL", |i| Some(i).zip(None::<i64>).map(|(a, b)| a != b)),
        ("id != NULL", |i| Some(i).zip(None::<i64>).map(|(a, b)| a != b)),
        ("score <> NULL", |i| score(i).zip(None::<i64>).map(|(a, b)| a != b)),
        ("score != NULL", |i| score(i).zip(None::<i64>).map(|(a, b)| a != b)),
    ];
    for (sql, predicate) in predicates {
        assert_eq!(
            run(&mut s, &format!("SELECT id FROM scan_rows WHERE {sql} ORDER BY id")),
            ids((0..29).filter(|i| predicate(*i) == Some(true))),
            "{sql}"
        );
    }
}

#[test]
fn membership_predicates_match_sql_three_value_logic() {
    let (_a, mut s) = db();
    fixture(&mut s, 29);
    type Predicate = (&'static str, fn(i64) -> Option<bool>);
    let predicates: [Predicate; 12] = [
        ("score IN (1, 2, 5)", |i| sql_in(score(i), &[Some(1), Some(2), Some(5)])),
        ("score IN (1, NULL)", |i| sql_in(score(i), &[Some(1), None])),
        ("score NOT IN (1, 2)", |i| sql_in(score(i), &[Some(1), Some(2)]).map(|v| !v)),
        ("score NOT IN (1, NULL)", |i| sql_in(score(i), &[Some(1), None]).map(|v| !v)),
        ("id IN (1, 3, 8)", |i| sql_in(Some(i), &[Some(1), Some(3), Some(8)])),
        ("id NOT IN (1, 3, 8)", |i| sql_in(Some(i), &[Some(1), Some(3), Some(8)]).map(|v| !v)),
        ("score IN (NULL)", |i| sql_in(score(i), &[None])),
        ("score NOT IN (NULL)", |i| sql_in(score(i), &[None]).map(|v| !v)),
        ("NOT score IN (1, 2)", |i| sql_in(score(i), &[Some(1), Some(2)]).map(|v| !v)),
        ("score IN (abs(1), CAST(2 AS BIGINT), bucket)", |i| {
            sql_in(score(i), &[Some(1), Some(2), Some(i % 3)])
        }),
        ("score IN (1, NULL) OR bucket=1", |i| {
            sql_or(sql_in(score(i), &[Some(1), None]), Some(i % 3 == 1))
        }),
        ("score NOT IN (1, NULL) OR bucket=1", |i| {
            sql_or(sql_in(score(i), &[Some(1), None]).map(|v| !v), Some(i % 3 == 1))
        }),
    ];
    for (sql, predicate) in predicates {
        assert_eq!(
            run(&mut s, &format!("SELECT id FROM scan_rows WHERE {sql} ORDER BY id")),
            ids((0..29).filter(|i| predicate(*i) == Some(true))),
            "{sql}"
        );
    }
}

#[test]
fn membership_parameters_infer_bigint_and_preserve_nulls() {
    let (_a, mut s) = db();
    fixture(&mut s, 29);
    let prepared = s.prepare("SELECT id FROM scan_rows WHERE score IN ($1, $2) ORDER BY id").unwrap();
    assert_eq!(s.describe(&prepared).unwrap().parameters, vec![DataType::BigInt, DataType::BigInt]);
    for (second, expected_second) in [(V::Int(3), Some(3)), (V::Null, None)] {
        let rows: Vec<_> = s.execute_prepared(&prepared, &[V::Int(1), second], None)
            .unwrap().rows.into_iter().map(|r| r.values).collect();
        assert_eq!(
            rows,
            ids((0..29).filter(|i| sql_in(score(*i), &[Some(1), expected_second]) == Some(true)))
        );
    }
}

#[test]
fn between_predicates_match_sql_three_value_logic_and_keep_key_range_scan() {
    let (_a, mut s) = db();
    fixture(&mut s, 29);
    type Predicate = (&'static str, fn(i64) -> Option<bool>);
    let predicates: [Predicate; 16] = [
        ("id BETWEEN 3 AND 7", |i| sql_and(Some(i >= 3), Some(i <= 7))),
        ("id NOT BETWEEN 3 AND 7", |i| sql_or(Some(i < 3), Some(i > 7))),
        ("score BETWEEN 1 AND 3", |i| sql_and(score(i).map(|v| v >= 1), score(i).map(|v| v <= 3))),
        ("score NOT BETWEEN 1 AND 3", |i| sql_or(score(i).map(|v| v < 1), score(i).map(|v| v > 3))),
        ("id BETWEEN 7 AND 3", |i| sql_and(Some(i >= 7), Some(i <= 3))),
        ("id NOT BETWEEN 7 AND 3", |i| sql_or(Some(i < 7), Some(i > 3))),
        ("score BETWEEN 7 AND 3", |i| sql_and(score(i).map(|v| v >= 7), score(i).map(|v| v <= 3))),
        ("score NOT BETWEEN 7 AND 3", |i| sql_or(score(i).map(|v| v < 7), score(i).map(|v| v > 3))),
        ("id BETWEEN ASYMMETRIC 3 AND 7", |i| sql_and(Some(i >= 3), Some(i <= 7))),
        ("score NOT BETWEEN ASYMMETRIC 1 AND 3", |i| sql_or(score(i).map(|v| v < 1), score(i).map(|v| v > 3))),
        ("score BETWEEN 1 AND 3 AND bucket=1", |i| {
            sql_and(sql_and(score(i).map(|v| v >= 1), score(i).map(|v| v <= 3)), Some(i % 3 == 1))
        }),
        ("score NOT BETWEEN 1 AND 3 OR bucket=1", |i| {
            sql_or(sql_or(score(i).map(|v| v < 1), score(i).map(|v| v > 3)), Some(i % 3 == 1))
        }),
        ("score BETWEEN 1 AND 3 OR bucket=1", |i| {
            sql_or(sql_and(score(i).map(|v| v >= 1), score(i).map(|v| v <= 3)), Some(i % 3 == 1))
        }),
        ("(score IN (1, 2) OR bucket=1) AND id BETWEEN 3 AND 20", |i| {
            sql_and(sql_or(sql_in(score(i), &[Some(1), Some(2)]), Some(i % 3 == 1)), sql_and(Some(i >= 3), Some(i <= 20)))
        }),
        ("score BETWEEN NULL AND 3", |i| sql_and(None, score(i).map(|v| v <= 3))),
        ("score NOT BETWEEN 1 AND NULL", |i| sql_or(score(i).map(|v| v < 1), None)),
    ];
    for (sql, predicate) in predicates {
        assert_eq!(
            run(&mut s, &format!("SELECT id FROM scan_rows WHERE {sql} ORDER BY id")),
            ids((0..29).filter(|i| predicate(*i) == Some(true))),
            "{sql}"
        );
    }
    let plan = run(&mut s, "EXPLAIN SELECT id FROM scan_rows WHERE id BETWEEN 3 AND 7");
    assert!(matches!(plan.first().and_then(|row| row.first()), Some(V::Text(text)) if text.starts_with("PrimaryKeyRangeScan")), "{plan:?}");
}

#[test]
fn thousand_literal_membership_list_keeps_bounded_filter_depth() {
    let (_a, mut s) = db();
    fixture(&mut s, 37);
    let candidates: Vec<_> = (0..1000).map(|i| Some(i * 2)).collect();
    let literals = (0..1000).map(|i| (i * 2).to_string()).collect::<Vec<_>>().join(", ");
    let sql = format!("SELECT id FROM scan_rows WHERE id IN ({literals}) ORDER BY id");
    assert_eq!(
        run(&mut s, &sql),
        ids((0..37).filter(|i| sql_in(Some(*i), &candidates) == Some(true)))
    );
}

#[test]
fn membership_and_between_reject_empty_lists_subqueries_and_symmetric_ranges() {
    let (_a, mut s) = db();
    fixture(&mut s, 3);
    for predicate in ["id IN ()", "id NOT IN ()"] {
        assert_eq!(
            s.execute(&format!("SELECT id FROM scan_rows WHERE {predicate}"), &[], None),
            Err(CoreError::Sql(E::Syntax)),
            "{predicate}"
        );
    }
    for predicate in [
        "id IN (SELECT id FROM scan_rows)",
        "id NOT IN (SELECT id FROM scan_rows)",
        "id BETWEEN SYMMETRIC 3 AND 7",
        "id NOT BETWEEN SYMMETRIC 3 AND 7",
        "id ! 1",
    ] {
        assert_eq!(
            s.execute(&format!("SELECT id FROM scan_rows WHERE {predicate}"), &[], None),
            Err(CoreError::Sql(E::Unsupported)),
            "{predicate}"
        );
    }
}

#[test]
fn limit_zero_skips_runtime_column_errors_but_not_bind_constants() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE lz (id BIGINT PRIMARY KEY, b BIGINT)");
    s.execute("INSERT INTO lz VALUES(1,$1)", &[V::Int(i64::MIN)], None)
        .unwrap();
    // Column-dependent runtime errors: raised when rows are produced, skipped
    // under LIMIT 0 because no input, filter or aggregate work runs.
    for q in [
        "SELECT sum(abs(b)) FROM lz",
        "SELECT abs(b) FROM lz",
        "SELECT abs(b) FROM lz WHERE abs(b) > 0",
        "SELECT id FROM lz WHERE abs(b) > 0",
        "SELECT id FROM lz ORDER BY abs(b)",
    ] {
        assert_eq!(
            s.execute(q, &[], None),
            Err(CoreError::Sql(E::Overflow)),
            "{q}"
        );
        let zero = s.execute(&format!("{q} LIMIT 0"), &[], None).expect(q);
        assert!(zero.rows.is_empty(), "{q} LIMIT 0");
        assert!(!zero.columns.is_empty(), "{q} LIMIT 0 keeps its description");
    }
    // Prepared LIMIT $1 bound to 0 behaves the same.
    let p = s.prepare("SELECT sum(abs(b)) FROM lz LIMIT $1").unwrap();
    assert!(s.execute_prepared(&p, &[V::Int(0)], None).unwrap().rows.is_empty());
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(1)], None),
        Err(CoreError::Sql(E::Overflow))
    );
    // Bind-phase immutable constants still fail under LIMIT 0 (unchanged control).
    let p = s.prepare("SELECT abs(-2147483648) FROM lz LIMIT 0").unwrap();
    assert_eq!(
        s.validate_immutable_constants(&p),
        Err(CoreError::Sql(E::Overflow))
    );
}

#[test]
fn in_list_expansion_is_admitted_before_cloning_the_lhs() {
    let (_a, mut s) = db();
    fixture(&mut s, 7);
    let limit = |s: &mut SqlSession, q: &str| {
        assert_eq!(s.execute(q, &[], None), Err(CoreError::Sql(E::Limit)), "{q}");
    };
    // Reviewer witness (GD-0058 / DD-0068 grammar review): every routine arity
    // <= 64 and scalar depth 2, but a 977-node lhs cloned 1000 times.
    let inner = format!("coalesce({})", vec!["0"; 60].join(","));
    let lhs = format!("coalesce({})", vec![inner.as_str(); 16].join(","));
    let list = vec!["0"; 1000].join(",");
    limit(&mut s, &format!("SELECT 1 WHERE {lhs} IN ({list})"));
    limit(&mut s, &format!("SELECT 1 WHERE {lhs} NOT IN ({list})"));
    // Node boundary: a 4-node lhs (3 calls + column) x 1024 = 4096 is admitted;
    // 1025 copies are rejected.
    let zeros = |n: usize| vec!["0"; n].join(",");
    let q = |n: usize| {
        format!("SELECT id FROM scan_rows WHERE abs(abs(abs(score))) IN ({}) ORDER BY id", zeros(n))
    };
    let expected: Vec<_> = (0..7).filter(|i| score(*i) == Some(0)).collect();
    assert_eq!(run(&mut s, &q(1024)), ids(expected));
    limit(&mut s, &q(1025));
    // Payload boundary: a 64-byte literal lhs x 1024 = 65536 bytes is admitted;
    // 1025 copies are rejected even though the node count is small.
    let wide = format!("'{}'", "x".repeat(64));
    let texts = |n: usize| vec!["'a'"; n].join(",");
    assert!(run(&mut s, &format!("SELECT 1 WHERE {wide} IN ({})", texts(1024))).is_empty());
    limit(&mut s, &format!("SELECT 1 WHERE {wide} IN ({})", texts(1025)));
    // Simple lists stay accepted with correct results.
    let q512 = format!(
        "SELECT id FROM scan_rows WHERE id IN ({}) ORDER BY id",
        (0..512).map(|i| i.to_string()).collect::<Vec<_>>().join(",")
    );
    assert_eq!(run(&mut s, &q512), ids(0..7));
    // BETWEEN admits its two lhs copies through the same check.
    assert_eq!(
        run(&mut s, "SELECT id FROM scan_rows WHERE abs(id) BETWEEN 2 AND 4 ORDER BY id"),
        ids(2..5)
    );
}
