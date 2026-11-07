mod common;
use common::*;
use giggles_core::{
    relational::{DataType as T, Value as V, *},
    *,
};
fn float(v: f64) -> V {
    V::Float8(Float8::from_bits(v.to_bits()))
}

// Oracle: pinned PostgreSQL 18.0, actual psql Parse0 and text Bind observations
// in target-dd68-prepared-contract/oracle.{sql,log}. Core execute_prepared takes
// typed Values; passing Text here is NOT equivalent to PG wire text Bind format.
#[test]
fn preferred_abs_explicit_casts_and_parameter_order() {
    let a = Admin::create(fresh("prepared-contract"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for (sql, params, columns, input, expected) in [
        (
            "SELECT abs($1)",
            vec![T::DoublePrecision],
            vec![T::DoublePrecision],
            vec![float(-2.5)],
            vec![float(2.5)],
        ),
        (
            "SELECT abs($1),$1::integer",
            vec![T::DoublePrecision],
            vec![T::DoublePrecision, T::Integer],
            vec![float(-2.5)],
            vec![float(2.5), V::Int(-2)],
        ),
        (
            "SELECT $1::integer,abs($1)",
            vec![T::Integer],
            vec![T::Integer, T::Integer],
            vec![V::Int(-7)],
            vec![V::Int(-7), V::Int(7)],
        ),
        (
            "SELECT abs($1::numeric),abs($2::integer),CAST($3 AS bytea)",
            vec![T::Numeric, T::Integer, T::Bytes],
            vec![T::Numeric, T::Integer, T::Bytes],
            vec![
                V::Numeric(Numeric::parse("-2.50").unwrap()),
                V::Int(-7),
                V::Bytes(b"abc".to_vec()),
            ],
            vec![
                V::Numeric(Numeric::parse("2.50").unwrap()),
                V::Int(7),
                V::Bytes(b"abc".to_vec()),
            ],
        ),
        (
            "SELECT abs($1),abs(NULL),abs('-2.5'),lower(upper($2::text))",
            vec![T::DoublePrecision, T::Text],
            vec![
                T::DoublePrecision,
                T::DoublePrecision,
                T::DoublePrecision,
                T::Text,
            ],
            vec![float(f64::NEG_INFINITY), V::Text("AbC".into())],
            vec![
                float(f64::INFINITY),
                V::Null,
                float(2.5),
                V::Text("abc".into()),
            ],
        ),
    ] {
        let p = s.prepare(sql).unwrap();
        let d = s.describe(&p).unwrap();
        assert_eq!(d.parameters, params, "{sql}");
        assert_eq!(
            d.columns
                .iter()
                .map(|c| c.data_type.clone())
                .collect::<Vec<_>>(),
            columns,
            "{sql}"
        );
        assert_eq!(
            s.execute_prepared(&p, &input, None).unwrap().rows[0].values,
            expected,
            "{sql}"
        );
        assert_eq!(
            s.execute_prepared(&p, &vec![V::Null; input.len()], None)
                .unwrap()
                .rows[0]
                .values,
            if input.len() == 2 {
                vec![V::Null, V::Null, float(2.5), V::Null]
            } else {
                vec![V::Null; d.columns.len()]
            },
            "{sql}"
        );
    }
    let p = s.prepare("SELECT CAST($1 AS bytea)").unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![T::Bytes]);
    assert!(
        s.execute_prepared(&p, &[V::Text("abc".into())], None)
            .is_err()
    );
    let p = s.prepare("SELECT abs($1)").unwrap();
    assert!(
        s.execute_prepared(&p, &[V::Text("-2.5".into())], None)
            .is_err()
    );
}
