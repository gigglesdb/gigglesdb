//! PostgreSQL 18.6 oracle: e3-query/wo-join-left/pg-probes.txt.
use giggles_core::{
    relational::{DataType as T, SqlError as E, SqlResult, Value as V},
    *,
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
type Pair = (i64, Option<i64>);
type PredicateCase<'a> = (&'a str, &'a [Pair]);
struct Db {
    session: Option<SqlSession>,
    admin: Option<Admin>,
    path: PathBuf,
}
impl Db {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "gd-joins-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let session = admin.sql_session();
        Self {
            session: Some(session),
            admin: Some(admin),
            path,
        }
    }
    fn session(&mut self) -> &mut SqlSession {
        self.session.as_mut().unwrap()
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        drop(self.session.take());
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn run(s: &mut SqlSession, q: &str) -> SqlResult {
    s.execute(q, &[], None)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"))
}
fn rows(s: &mut SqlSession, q: &str) -> Vec<Vec<V>> {
    run(s, q).rows.into_iter().map(|r| r.values).collect()
}
fn pairs(values: &[(i64, Option<i64>)]) -> Vec<Vec<V>> {
    values
        .iter()
        .map(|&(a, b)| vec![V::Int(a), b.map_or(V::Null, V::Int)])
        .collect()
}
fn fixture(s: &mut SqlSession) {
    run(s, "CREATE TABLE a(id int PRIMARY KEY,k int,v text)");
    run(s, "CREATE TABLE b(id int PRIMARY KEY,k int,v text)");
    run(s, "CREATE TABLE c(id int PRIMARY KEY,bid int)");
    run(
        s,
        "INSERT INTO a VALUES(1,10,'alpha'),(2,20,'beta'),(3,NULL,NULL)",
    );
    run(
        s,
        "INSERT INTO b VALUES(11,10,'apple'),(12,10,NULL),(13,30,'berry')",
    );
    run(s, "INSERT INTO c VALUES(21,11),(22,13)");
}
#[test]
fn boolean_on_predicates_match_postgres() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    let cases: &[PredicateCase<'_>] = &[
        ("JOIN b ON a.k=b.k AND b.id>11", &[(1, Some(12))]),
        (
            "JOIN b ON a.k=b.k OR a.id=2",
            &[
                (1, Some(11)),
                (1, Some(12)),
                (2, Some(11)),
                (2, Some(12)),
                (2, Some(13)),
            ],
        ),
        ("JOIN b ON a.k<b.k", &[(1, Some(13)), (2, Some(13))]),
        (
            "LEFT OUTER JOIN b ON b.k BETWEEN a.k AND a.k+10",
            &[(1, Some(11)), (1, Some(12)), (2, Some(13)), (3, None)],
        ),
        (
            "LEFT JOIN b ON b.v IS NULL",
            &[(1, Some(12)), (2, Some(12)), (3, Some(12))],
        ),
        (
            "LEFT JOIN b ON b.v LIKE 'a%'",
            &[(1, Some(11)), (2, Some(11)), (3, Some(11))],
        ),
        ("LEFT JOIN b ON false", &[(1, None), (2, None), (3, None)]),
        ("LEFT JOIN b ON NULL", &[(1, None), (2, None), (3, None)]),
        (
            "LEFT JOIN b ON a.k=b.k AND b.id=12",
            &[(1, Some(12)), (2, None), (3, None)],
        ),
        ("LEFT JOIN b ON a.k=b.k WHERE b.id=12", &[(1, Some(12))]),
        ("JOIN b ON a.k=b.k WHERE a.k IS NULL", &[]),
    ];
    for (join, expected) in cases {
        let sql = format!("SELECT a.id,b.id FROM a {join} ORDER BY 1,2");
        assert_eq!(rows(s, &sql), pairs(expected), "{sql}");
    }
    assert_eq!(
        rows(
            s,
            "SELECT a.id,b.id FROM a LEFT JOIN b ON true ORDER BY 1,2"
        ),
        pairs(&[
            (1, Some(11)),
            (1, Some(12)),
            (1, Some(13)),
            (2, Some(11)),
            (2, Some(12)),
            (2, Some(13)),
            (3, Some(11)),
            (3, Some(12)),
            (3, Some(13))
        ])
    );
}
#[test]
fn chained_left_joins_preserve_null_extensions_and_nullable_counts() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    assert_eq!(
        rows(
            s,
            "SELECT a.id,b.id,c.id FROM a LEFT JOIN b ON a.k=b.k LEFT JOIN c ON b.id=c.bid ORDER BY 1,2,3"
        ),
        vec![
            vec![V::Int(1), V::Int(11), V::Int(21)],
            vec![V::Int(1), V::Int(12), V::Null],
            vec![V::Int(2), V::Null, V::Null],
            vec![V::Int(3), V::Null, V::Null]
        ]
    );
    assert_eq!(
        rows(
            s,
            "SELECT a.id,count(*),count(b.id) FROM a LEFT JOIN b ON a.k=b.k GROUP BY a.id ORDER BY 1"
        ),
        vec![
            vec![V::Int(1), V::Int(2), V::Int(2)],
            vec![V::Int(2), V::Int(1), V::Int(0)],
            vec![V::Int(3), V::Int(1), V::Int(0)]
        ]
    );
}
#[test]
fn typed_key_comparisons_use_sql_numeric_semantics() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    run(s, "CREATE TABLE big(id bigint PRIMARY KEY)");
    run(s, "INSERT INTO big VALUES(1),(2)");
    run(s, "CREATE TABLE flt(id double precision PRIMARY KEY)");
    run(s, "INSERT INTO flt VALUES(1),(2.5)");
    assert_eq!(
        rows(
            s,
            "SELECT a.id,big.id FROM a JOIN big ON a.id=big.id ORDER BY 1"
        ),
        pairs(&[(1, Some(1)), (2, Some(2))])
    );
    assert_eq!(
        rows(s, "SELECT a.id FROM a JOIN flt ON a.id=flt.id ORDER BY 1"),
        vec![vec![V::Int(1)]]
    );
    for sql in [
        "SELECT a.id,b.id FROM a JOIN b ON a.id=b.v",
        "SELECT a.id,b.id FROM a JOIN b ON missing_join_routine(a.id)=b.id",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::UndefinedFunction)),
            "{sql}"
        );
    }
}
#[test]
fn nonkey_float_exact_scan_joins_match_postgres_precision_coercion() {
    let mut db = Db::new();
    let s = db.session();
    run(
        s,
        "CREATE TABLE float_outer(id int PRIMARY KEY,v double precision)",
    );
    run(s, "INSERT INTO float_outer VALUES(1,9007199254740992.0)");
    for (table, ty) in [("bigint_inner", "bigint"), ("numeric_inner", "numeric")] {
        run(
            s,
            &format!("CREATE TABLE {table}(id int PRIMARY KEY,v {ty})"),
        );
        run(
            s,
            &format!("INSERT INTO {table} VALUES(1,9007199254740992),(2,9007199254740993)"),
        );
        // PG18.6 converts the exact operand to float8, so both values match.
        // Oracle: e3-query/wo-join-left/followup-pg-probes.txt.
        for predicate in [format!("f.v={table}.v"), format!("{table}.v=f.v")] {
            for clause in [
                format!("ON {predicate}"),
                format!("ON true WHERE {predicate}"),
            ] {
                let sql = format!(
                    "SELECT f.id,{table}.id FROM float_outer f JOIN {table} {clause} ORDER BY 1,2"
                );
                assert_eq!(rows(s, &sql), pairs(&[(1, Some(1)), (1, Some(2))]), "{sql}");
                let plan = format!("{:?}", rows(s, &format!("EXPLAIN {sql}")));
                assert!(plan.contains("InnerScanNestedLoop"), "{sql}: {plan}");
            }
        }
    }
}
#[test]
fn scan_join_above_legacy_cap_succeeds_and_cancellation_is_scoped() {
    let mut db = Db::new();
    let s = db.session();
    run(s, "CREATE TABLE l(id int PRIMARY KEY)");
    run(s, "CREATE TABLE r(id int PRIMARY KEY,l_id int)");
    for start in [1, 101, 201] {
        let values = (start..start + 100)
            .map(|n| format!("({n})"))
            .collect::<Vec<_>>()
            .join(",");
        run(s, &format!("INSERT INTO l VALUES {values}"));
        let values = (start..start + 100)
            .map(|n| format!("({n},{n})"))
            .collect::<Vec<_>>()
            .join(",");
        run(s, &format!("INSERT INTO r VALUES {values}"));
    }
    // The non-key equality scans 90,000 candidates above the former 65,536 cap.
    assert_eq!(
        rows(
            s,
            "SELECT l.id,r.id FROM l LEFT JOIN r ON l.id=r.l_id ORDER BY l.id"
        ),
        (1..=300)
            .map(|n| vec![V::Int(n), V::Int(n)])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        rows(s, "SELECT l.id FROM l JOIN r ON false LIMIT 1"),
        Vec::<Vec<V>>::new()
    );
    assert_eq!(
        rows(s, "SELECT l.id FROM l LEFT JOIN r ON false LIMIT 1"),
        vec![vec![V::Int(1)]]
    );
    assert_eq!(
        rows(s, "SELECT l.id FROM l LEFT JOIN r ON false ORDER BY l.id"),
        (1..=300).map(|n| vec![V::Int(n)]).collect::<Vec<_>>()
    );
    for sql in [
        "SELECT l.id FROM l JOIN r ON false LIMIT 1",
        "SELECT l.id FROM l LEFT JOIN r ON false ORDER BY l.id LIMIT 1",
    ] {
        let cancellation = Cancellation::default();
        assert!(cancellation.cancel());
        assert_eq!(
            cancellation.scope(|| s.execute(sql, &[], None)),
            Err(CoreError::Cancelled)
        );
    }
    assert_eq!(
        rows(s, "SELECT id FROM l WHERE id=1"),
        vec![vec![V::Int(1)]]
    );
}
#[test]
fn on_parameters_are_bound_before_predicate_evaluation() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    let p = s
        .prepare("SELECT a.id,b.id FROM a LEFT JOIN b ON a.k=b.k AND b.id=$1 ORDER BY 1,2")
        .unwrap();
    let result = s.execute_prepared(&p, &[V::Int(12)], None).unwrap();
    assert_eq!(
        result
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        pairs(&[(1, Some(12)), (2, None), (3, None)])
    );
    let p = s
        .prepare("SELECT a.id,b.id FROM a LEFT JOIN b ON $1 ORDER BY 1,2")
        .unwrap();
    let result = s.execute_prepared(&p, &[V::Bool(false)], None).unwrap();
    assert_eq!(
        result
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        pairs(&[(1, None), (2, None), (3, None)])
    );
}
#[test]
fn unkeyed_join_on_parameter_infers_bigint_from_both_sides() {
    // PG18.6: $1 compared with b.i and a.i (bigint) is inferred as int8 (OID 20).
    let mut db = Db::new();
    let s = db.session();
    run(s, "CREATE TABLE f(i BIGINT PRIMARY KEY)");
    run(s, "INSERT INTO f VALUES(1),(2)");
    let p = s
        .prepare("SELECT abs($1) FROM f a JOIN f b ON b.i=$1 WHERE a.i=$1 LIMIT 20")
        .unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, [T::BigInt]);
    let result = s.execute_prepared(&p, &[V::Int(1)], None).unwrap();
    assert_eq!(
        result
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        vec![vec![V::Int(1)]]
    );
}
