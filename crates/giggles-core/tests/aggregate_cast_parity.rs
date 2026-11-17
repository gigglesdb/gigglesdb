#![cfg(feature = "engine-rocksdb")]
//! PostgreSQL 18.6 oracle: r5/hd/hd-6a/pg18.txt (GROUP BY/HAVING/ORDER BY).
use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{DataType as T, SqlResult, Value as V},
};

fn assert_result(result: &SqlResult, names: &[&str], rows: &[Vec<V>]) {
    assert_eq!(
        result
            .columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        names
    );
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        rows
    );
}

fn parity(s: &mut SqlSession, sql: &str, names: &[&str], types: &[T], rows: &[Vec<V>]) {
    let result = s
        .execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_result(&result, names, rows);
    let prepared = s
        .prepare(sql)
        .unwrap_or_else(|e| panic!("prepare {sql}: {e:?}"));
    let description = s.describe(&prepared).unwrap();
    assert_eq!(
        description
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        names
    );
    assert_eq!(
        description
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        types
    );
    let result = s.execute_prepared(&prepared, &[], None).unwrap();
    assert_result(&result, names, rows);
}

#[test]
fn aggregate_cast_values_names_and_types_in_both_execution_paths() {
    let path = std::env::temp_dir().join(format!("hd6a-aggregate-cast-{}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE t(a int)", &[], None).unwrap();
    s.execute("INSERT INTO t VALUES (1),(2)", &[], None)
        .unwrap();
    for (expression, name, ty, value) in [
        ("count(*)::bigint", "count", T::BigInt, V::Int(2)),
        ("count(*)::int8", "count", T::BigInt, V::Int(2)),
        ("count(a)::bigint", "count", T::BigInt, V::Int(2)),
        ("sum(a)::bigint", "sum", T::BigInt, V::Int(3)),
        ("count(*)::int", "count", T::Integer, V::Int(2)),
        ("cast(count(*) as bigint)", "count", T::BigInt, V::Int(2)),
        ("(count(*))::text", "count", T::Text, V::Text("2".into())),
        (
            "((count(*)))::bigint::text",
            "count",
            T::Text,
            V::Text("2".into()),
        ),
        ("cast(((sum(a))) as bigint)", "sum", T::BigInt, V::Int(3)),
        ("count(*)::bigint AS total", "total", T::BigInt, V::Int(2)),
        (
            "cast(count(*) as bigint) AS total",
            "total",
            T::BigInt,
            V::Int(2),
        ),
    ] {
        parity(
            &mut s,
            &format!("SELECT {expression} FROM t"),
            &[name],
            &[ty],
            &[vec![value]],
        );
    }
    for expression in ["a::bigint", "cast(a as bigint)", "((a))::bigint"] {
        parity(
            &mut s,
            &format!("SELECT {expression} FROM t ORDER BY a"),
            &["a"],
            &[T::BigInt],
            &[vec![V::Int(1)], vec![V::Int(2)]],
        );
    }
    for sql in [
        "SELECT a,count(*)::bigint FROM t GROUP BY a ORDER BY a",
        "SELECT a,count(*)::bigint FROM t GROUP BY a ORDER BY count(*)::bigint,a",
    ] {
        parity(
            &mut s,
            sql,
            &["a", "count"],
            &[T::Integer, T::BigInt],
            &[vec![V::Int(1), V::Int(1)], vec![V::Int(2), V::Int(1)]],
        );
    }
    parity(
        &mut s,
        "SELECT count(*)::bigint FROM t HAVING count(*)::bigint > 1",
        &["count"],
        &[T::BigInt],
        &[vec![V::Int(2)]],
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
