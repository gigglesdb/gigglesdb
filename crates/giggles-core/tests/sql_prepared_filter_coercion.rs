mod common;
use common::*;
use giggles_core::{
    relational::{DataType as T, Value as V, *},
    *,
};
fn float(n: f64) -> V {
    V::Float8(Float8::from_bits(n.to_bits()))
}
fn db() -> (Admin, SqlSession) {
    let a = Admin::create(fresh("prepared-filter"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute("CREATE TABLE f(i BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    for n in [0i64, 1, 16777217, 9007199254740992, 9007199254740993] {
        s.execute(&format!("INSERT INTO f VALUES({n})"), &[], None)
            .unwrap();
    }
    (a, s)
}
// Actual pinned PG18 psql Parse0/Bind results: target-dd68-prepared-filter/oracle.log.
// Core inputs remain typed Values, never wire text Bind strings.
#[test]
fn mixed_key_comparison_keeps_all_rounded_matches_and_typed_contract() {
    let (_a, mut s) = db();
    let p = s
        .prepare("SELECT abs($1),i FROM f WHERE i=$1 ORDER BY i LIMIT 20")
        .unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![T::DoublePrecision]);
    for (input, keys) in [
        (
            float(9007199254740992.),
            vec![9007199254740992, 9007199254740993],
        ),
        (float(0.5), vec![]),
        (float(-0.), vec![0]),
        (float(f64::NAN), vec![]),
        (float(f64::INFINITY), vec![]),
        (V::Null, vec![]),
    ] {
        let result = s.execute_prepared(&p, &[input], None).unwrap();
        assert_eq!(
            result
                .rows
                .iter()
                .map(|r| r.values[1].clone())
                .collect::<Vec<_>>(),
            keys.into_iter().map(V::Int).collect::<Vec<_>>()
        );
    }
    assert!(s.execute_prepared(&p, &[V::Int(1)], None).is_err());
    assert!(
        s.execute_prepared(&p, &[V::Text("1".into())], None)
            .is_err()
    );
}
#[test]
fn numeric_promotion_cast_order_and_three_valued_logic() {
    let (_a, mut s) = db();
    for (sql, input, expected) in [
        (
            "SELECT $1::real FROM f WHERE i=$1 LIMIT 20",
            V::Float4(Float4::from_bits(16777216f32.to_bits())),
            0,
        ),
        (
            "SELECT $1::numeric FROM f WHERE i=$1 LIMIT 20",
            V::Numeric(Numeric::parse("16777217").unwrap()),
            1,
        ),
        (
            "SELECT $1::smallint FROM f WHERE i=$1 LIMIT 20",
            V::Int(1),
            1,
        ),
        (
            "SELECT abs($1),$1::integer FROM f WHERE i=$1 LIMIT 20",
            float(0.5),
            0,
        ),
        (
            "SELECT $1::integer,abs($1) FROM f WHERE i=$1 LIMIT 20",
            V::Int(1),
            1,
        ),
        ("SELECT abs($1) FROM f WHERE NOT(i=$1) LIMIT 20", V::Null, 0),
        (
            "SELECT abs($1) FROM f WHERE i=$1 OR i=1 LIMIT 20",
            V::Null,
            1,
        ),
        ("SELECT abs($1) FROM f WHERE i=$1 LIMIT 0", float(1.), 0),
    ] {
        let p = s.prepare(sql).unwrap();
        assert_eq!(
            s.execute_prepared(&p, &[input], None).unwrap().rows.len(),
            expected,
            "{sql}"
        );
    }
    s.execute(
        "CREATE TABLE r(id BIGINT PRIMARY KEY,r REAL,n NUMERIC)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO r VALUES(1,16777216,16777217)", &[], None)
        .unwrap();
    for sql in [
        "SELECT $1::numeric FROM r WHERE r=$1 LIMIT 20",
        "SELECT $1::real FROM r WHERE n=$1 LIMIT 20",
    ] {
        let input = if sql.contains("::numeric") {
            V::Numeric(Numeric::parse("16777217").unwrap())
        } else {
            V::Float4(Float4::from_bits(16777216f32.to_bits()))
        };
        let p = s.prepare(sql).unwrap();
        assert!(
            s.execute_prepared(&p, &[input], None)
                .unwrap()
                .rows
                .is_empty(),
            "{sql}"
        );
    }
}
#[test]
fn aliases_order_ranges_and_indexed_join_still_filter() {
    let (_a, mut s) = db();
    for (op, count) in [("=", 2), ("<", 3), ("<=", 5), (">", 0), (">=", 2)] {
        let sql = format!("SELECT abs($1),a.i FROM f a WHERE a.i{op}$1 ORDER BY a.i LIMIT 20");
        let p = s.prepare(&sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_eq!(
            s.execute_prepared(&p, &[float(9007199254740992.)], None)
                .unwrap()
                .rows
                .len(),
            count,
            "{sql}"
        );
    }
    let p = s
        .prepare(
            "SELECT abs($1),a.i FROM f a JOIN f b ON b.i=a.i WHERE a.i=$1 ORDER BY a.i LIMIT 20",
        )
        .unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[float(9007199254740992.)], None)
            .unwrap()
            .rows
            .len(),
        2
    );
    let p = s
        .prepare("SELECT abs($1),i FROM f WHERE NOT(i=$1) LIMIT 20")
        .unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[float(9007199254740992.)], None)
            .unwrap()
            .rows
            .len(),
        3
    );
    // A mixed numeric predicate cannot make an unkeyed join admissible.
    assert!(
        s.prepare("SELECT abs($1) FROM f a JOIN f b ON b.i=$1 WHERE a.i=$1 LIMIT 20")
            .is_err()
    );
    let p = s
        .prepare("SELECT $1::numeric FROM f WHERE i::real=$1 LIMIT 20")
        .unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[V::Numeric(Numeric::parse("1e400").unwrap())], None),
        Err(CoreError::Sql(SqlError::Overflow))
    );
}
