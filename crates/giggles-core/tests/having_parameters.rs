//! Parameter casts must survive grouping identity probes before inference.
mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits,
    relational::{DataType as T, Float8, Numeric, SqlError, Value as V},
};
fn numeric(s: &str) -> V {
    V::Numeric(Numeric::parse(s).unwrap())
}
fn exercise(engine: Engine) {
    let path = common::fresh("having-parameters");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE orders(id int PRIMARY KEY,item text,qty int,price numeric)",
        &[],
        None,
    )
    .unwrap();
    s.execute(
        "INSERT INTO orders VALUES(1,'a',1,1.5),(2,'a',2,2.5),(3,'b',5,NULL),(4,'c',NULL,3)",
        &[],
        None,
    )
    .unwrap();
    for aggregate in ["count(*)", "sum(qty)", "avg(qty)", "min(qty)", "max(qty)"] {
        for (cast, ty, value) in [
            ("numeric", T::Numeric, numeric("1")),
            (
                "float8",
                T::DoublePrecision,
                V::Float8(Float8::parse("1").unwrap()),
            ),
            ("int4", T::Integer, V::Int(1)),
            ("int8", T::BigInt, V::Int(1)),
        ] {
            for predicate in [
                format!("{aggregate} > $1::{cast}"),
                format!("$1::{cast} < {aggregate}"),
            ] {
                for sql in [
                    format!(
                        "SELECT item FROM orders GROUP BY item HAVING {predicate} ORDER BY item"
                    ),
                    format!("SELECT item,{predicate} FROM orders GROUP BY item ORDER BY item"),
                    format!("SELECT item FROM orders GROUP BY item ORDER BY {predicate},item"),
                ] {
                    for mode in ["cast", "declared", "untyped"] {
                        let query = if mode != "cast" {
                            sql.replace(&format!("::{cast}"), "")
                        } else {
                            sql.clone()
                        };
                        let p = s
                            .prepare(&query)
                            .unwrap_or_else(|e| panic!("{query}: {e:?}"));
                        let p = if mode == "declared" {
                            p.with_parameter_types(&[Some(ty.clone())]).unwrap()
                        } else {
                            p
                        };
                        let (ty, value) = if mode == "untyped" {
                            match aggregate {
                                "avg(qty)" => (T::Numeric, numeric("1")),
                                "min(qty)" | "max(qty)" => (T::Integer, V::Int(1)),
                                _ => (T::BigInt, V::Int(1)),
                            }
                        } else { (ty.clone(), value.clone()) };
                        assert_eq!(
                            s.describe(&p).unwrap().parameters,
                            vec![ty],
                            "{sql}"
                        );
                        let rows = s
                            .execute_prepared(&p, std::slice::from_ref(&value), None)
                            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
                            .rows
                            .into_iter()
                            .map(|r| r.values)
                            .collect::<Vec<_>>();
                        let (a, b, c) = match aggregate {
                            "count(*)" => (V::Bool(true), V::Bool(false), V::Bool(false)),
                            "min(qty)" => (V::Bool(false), V::Bool(true), V::Null),
                            _ => (V::Bool(true), V::Bool(true), V::Null),
                        };
                        let text = |x: &str| V::Text(x.into());
                        let expected = if sql.contains("HAVING") {
                            [("a", &a), ("b", &b), ("c", &c)]
                                .into_iter()
                                .filter(|(_, v)| **v == V::Bool(true))
                                .map(|(x, _)| vec![text(x)])
                                .collect::<Vec<_>>()
                        } else if sql.starts_with("SELECT item,") {
                            vec![vec![text("a"), a], vec![text("b"), b], vec![text("c"), c]]
                        } else if aggregate == "count(*)" {
                            vec![vec![text("b")], vec![text("c")], vec![text("a")]]
                        } else {
                            vec![vec![text("a")], vec![text("b")], vec![text("c")]]
                        };
                        assert_eq!(rows, expected, "{sql}");
                        assert_eq!(
                            s.execute_prepared(&p, &[], None),
                            Err(CoreError::Sql(SqlError::Parameters)),
                            "{sql}"
                        );
                    }
                }
            }
        }
    }
    for (predicate, ty, value, expected) in [
        (
            "count(*) > $1::numeric",
            T::Numeric,
            numeric("1"),
            vec!["a"],
        ),
        (
            "count(*) > $1::numeric",
            T::Numeric,
            numeric("0.5"),
            vec!["a", "b", "c"],
        ),
        (
            "$1::numeric < count(*)",
            T::Numeric,
            numeric("1"),
            vec!["a"],
        ),
        (
            "sum(qty) > $1::float8",
            T::DoublePrecision,
            V::Float8(Float8::parse("2.5").unwrap()),
            vec!["a", "b"],
        ),
        (
            "avg(qty) >= $1::numeric",
            T::Numeric,
            numeric("1.5"),
            vec!["a", "b"],
        ),
        ("sum(price) > $1::int4", T::Integer, V::Int(3), vec!["a"]),
        ("count(*) > $1", T::BigInt, V::Int(1), vec!["a"]),
        ("avg(qty) > $1", T::Numeric, numeric("1.5"), vec!["b"]),
        ("$1 < sum(price)", T::Numeric, numeric("3"), vec!["a"]),
    ] {
        let sql = format!("SELECT item FROM orders GROUP BY item HAVING {predicate} ORDER BY item");
        let p = s.prepare(&sql).unwrap();
        assert_eq!(s.describe(&p).unwrap().parameters, vec![ty]);
        let result = s.execute_prepared(&p, &[value], None).unwrap();
        assert_eq!(
            result
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            expected
                .into_iter()
                .map(|x| vec![V::Text(x.into())])
                .collect::<Vec<_>>(),
            "{sql}"
        );
    }
    for (aggregate, predicate, ty, value, expected) in [
        (
            "count(*)",
            "count(*) > $1",
            T::Numeric,
            numeric("1"),
            vec![vec![V::Text("a".into()), V::Int(2)]],
        ),
        (
            "sum(qty)",
            "sum(qty) > $1",
            T::DoublePrecision,
            V::Float8(Float8::parse("2.5").unwrap()),
            vec![
                vec![V::Text("a".into()), V::Int(3)],
                vec![V::Text("b".into()), V::Int(5)],
            ],
        ),
        (
            "avg(qty)",
            "avg(qty) >= $1",
            T::Numeric,
            numeric("1.5"),
            vec![
                vec![V::Text("a".into()), numeric("1.5000000000000000")],
                vec![V::Text("b".into()), numeric("5.0000000000000000")],
            ],
        ),
        (
            "sum(price)",
            "sum(price) > $1",
            T::Integer,
            V::Int(3),
            vec![vec![V::Text("a".into()), numeric("4.0")]],
        ),
    ] {
        let sql = format!(
            "SELECT item,{aggregate} FROM orders GROUP BY item HAVING {predicate} ORDER BY item"
        );
        let p = s
            .prepare(&sql)
            .unwrap()
            .with_parameter_types(&[Some(ty)])
            .unwrap();
        let rows = s
            .execute_prepared(&p, &[value], None)
            .unwrap()
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>();
        for (actual, expected) in rows.iter().zip(&expected) {
            for (actual, expected) in actual.iter().zip(expected) {
                if let (V::Numeric(a), V::Numeric(b)) = (actual, expected) {
                    assert_eq!(a.to_string(), b.to_string(), "{sql}");
                }
            }
        }
        assert_eq!(rows, expected, "{sql}");
    }
    let p = s
        .prepare("SELECT $1::int4,count(*) FROM orders HAVING count(*) > $2::numeric")
        .unwrap();
    assert_eq!(
        s.describe(&p).unwrap().parameters,
        vec![T::Integer, T::Numeric]
    );
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(7)], None),
        Err(CoreError::Sql(SqlError::Parameters))
    );
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(7), numeric("1")], None)
            .unwrap()
            .rows
            .len(),
        1
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn legacy() {
    exercise(Engine::Legacy);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2() {
    exercise(Engine::V2);
}
