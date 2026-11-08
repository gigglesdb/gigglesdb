//! DD-0068 F11: PG 18.6 no-FROM aggregate behaviour (oracle verified in postgres:18.6)
mod common;
use giggles_core::{
    relational::{DataType as T, Numeric, Prepared, SqlError as E, StatementClock, Value as V},
    *,
};

#[test]
fn pg18_no_from_aggregate_oracle() {
    let path = common::fresh("no-from-aggregates");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 1,
        statement_us: 1,
        zone: None,
        wall: None,
    };
    let n = |v| V::Numeric(Numeric::parse(v).unwrap());
    let one = |v| vec![vec![v]];
    type Case = (&'static str, Result<Vec<Vec<V>>, E>, Vec<T>);
    let cases: Vec<Case> = vec![
        ("SELECT count(*)", Ok(one(V::Int(1))), vec![T::BigInt]),
        (
            "SELECT avg(1)",
            Ok(one(n("1.00000000000000000000"))),
            vec![T::Numeric],
        ),
        (
            "SELECT avg(DISTINCT 1)",
            Ok(one(n("1.00000000000000000000"))),
            vec![T::Numeric],
        ),
        ("SELECT count(1)", Ok(one(V::Int(1))), vec![T::BigInt]),
        ("SELECT sum(1)", Ok(one(V::Int(1))), vec![T::BigInt]),
        ("SELECT min(3)", Ok(one(V::Int(3))), vec![T::Integer]),
        (
            "SELECT max('b')",
            Ok(one(V::Text("b".into()))),
            vec![T::Text],
        ),
        (
            "SELECT avg(2)",
            Ok(one(n("2.0000000000000000"))),
            vec![T::Numeric],
        ),
        (
            "SELECT count(DISTINCT 1)",
            Ok(one(V::Int(1))),
            vec![T::BigInt],
        ),
        (
            "SELECT sum(DISTINCT 2)",
            Ok(one(V::Int(2))),
            vec![T::BigInt],
        ),
        (
            "SELECT avg(DISTINCT 2)",
            Ok(one(n("2.0000000000000000"))),
            vec![T::Numeric],
        ),
        (
            "SELECT count(*) WHERE false",
            Ok(one(V::Int(0))),
            vec![T::BigInt],
        ),
        (
            "SELECT sum(1) WHERE false",
            Ok(one(V::Null)),
            vec![T::BigInt],
        ),
        (
            "SELECT max(1) WHERE false",
            Ok(one(V::Null)),
            vec![T::Integer],
        ),
        (
            "SELECT count(*) HAVING count(*)>1",
            Ok(vec![]),
            vec![T::BigInt],
        ),
        (
            "SELECT count(*) HAVING count(*)=1",
            Ok(one(V::Int(1))),
            vec![T::BigInt],
        ),
        ("SELECT count(*) HAVING false", Ok(vec![]), vec![T::BigInt]),
        (
            "SELECT position(max('b') IN 'abc')",
            Ok(one(V::Int(2))),
            vec![T::Integer],
        ),
        (
            "SELECT max(position('b' IN 'abc'))",
            Ok(one(V::Int(2))),
            vec![T::Integer],
        ),
        (
            "SELECT 1,count(*)",
            Ok(vec![vec![V::Int(1), V::Int(1)]]),
            vec![T::Integer, T::BigInt],
        ),
        (
            "SELECT count(*) ORDER BY 1",
            Ok(one(V::Int(1))),
            vec![T::BigInt],
        ),
        (
            "SELECT count(*) ORDER BY count(*)",
            Ok(one(V::Int(1))),
            vec![T::BigInt],
        ),
        (
            "SELECT count(*),'x' ORDER BY 2",
            Ok(vec![vec![V::Int(1), V::Text("x".into())]]),
            vec![T::BigInt, T::Text],
        ),
        ("SELECT count(*) LIMIT 0", Ok(vec![]), vec![T::BigInt]),
        (
            "SELECT count(*) LIMIT 1 OFFSET 1",
            Ok(vec![]),
            vec![T::BigInt],
        ),
        (
            "SELECT count(*) OFFSET 0",
            Ok(one(V::Int(1))),
            vec![T::BigInt],
        ),
        ("SELECT count(*)+1", Ok(one(V::Int(2))), vec![T::BigInt]),
        (
            "SELECT max('a'::text)||'z'",
            Ok(one(V::Text("az".into()))),
            vec![T::Text],
        ),
        (
            "SELECT count(*) AS n,sum(2) AS s",
            Ok(vec![vec![V::Int(1), V::Int(2)]]),
            vec![T::BigInt, T::BigInt],
        ),
        (
            "SELECT abs(avg(1))",
            Ok(one(n("1.00000000000000000000"))),
            vec![T::Numeric],
        ),
        (
            "SELECT CAST(avg(DISTINCT 1) AS numeric)",
            Ok(one(n("1.00000000000000000000"))),
            vec![T::Numeric],
        ),
        (
            "SELECT 1 HAVING count(*)>0",
            Ok(one(V::Int(1))),
            vec![T::Integer],
        ),
        ("SELECT 1 HAVING true", Ok(one(V::Int(1))), vec![T::Integer]),
        (
            "SELECT abs(1) ORDER BY count(*)",
            Ok(one(V::Int(1))),
            vec![T::Integer],
        ),
        ("SELECT avg(1) GROUP BY 1", Err(E::GroupingError), vec![]),
        ("SELECT count(*) GROUP BY 1", Err(E::GroupingError), vec![]),
        ("SELECT count(count(*))", Err(E::GroupingError), vec![]),
        ("SELECT sum(count(*))", Err(E::GroupingError), vec![]),
        (
            "SELECT count(1) WHERE count(1)>0",
            Err(E::GroupingError),
            vec![],
        ),
        ("SELECT max(NULL::int)", Ok(one(V::Null)), vec![T::Integer]),
        (
            "SELECT count(NULL::int)",
            Ok(one(V::Int(0))),
            vec![T::BigInt],
        ),
        (
            "SELECT nosuchfn(count(1))",
            Err(E::UndefinedFunction),
            vec![],
        ),
        (
            "SELECT count(*) WHERE nosuchfn(1)=1",
            Err(E::UndefinedFunction),
            vec![],
        ),
        // PG18.6: 42803 aggregate in WHERE / 42883 nosuchfn(bigint); F15 closed the F11 count(*) parser limit
        (
            "SELECT count(*) WHERE count(*)>0",
            Err(E::GroupingError),
            vec![],
        ),
        ("SELECT nosuchfn(count(*))", Err(E::UndefinedFunction), vec![]),
        ("CREATE TABLE so(id serial PRIMARY KEY)", Ok(vec![]), vec![]),
        (
            "SELECT count(*) FROM so WHERE count(*)>0",
            Err(E::GroupingError),
            vec![],
        ),
        (
            "SELECT count(nextval('so_id_seq'))",
            Ok(one(V::Int(1))),
            vec![T::BigInt],
        ),
        (
            "SELECT currval('so_id_seq')",
            Ok(one(V::Int(1))),
            vec![T::BigInt],
        ),
        (
            "SELECT sum(nextval('so_id_seq')) WHERE false",
            Ok(one(V::Null)),
            vec![T::Numeric],
        ),
        (
            "SELECT currval('so_id_seq')",
            Ok(one(V::Int(1))),
            vec![T::BigInt],
        ),
    ];
    let mut failures = Vec::new();
    let mut countstar_where_error = None;
    for (sql, expected, expected_types) in cases {
        let description = s.prepare(sql).and_then(|p| s.describe(&p)).map(|d| {
            d.columns
                .into_iter()
                .map(|c| c.data_type)
                .collect::<Vec<_>>()
        });
        let actual = s
            .execute_at(sql, &[], None, &clock)
            .map(|r| r.rows.into_iter().map(|r| r.values).collect::<Vec<_>>());
        if sql == "SELECT count(*) WHERE count(*)>0" {
            countstar_where_error = Some(actual.clone());
        } else if sql == "SELECT count(*) FROM so WHERE count(*)>0"
            && countstar_where_error.as_ref() != Some(&actual)
        {
            failures.push(format!(
                "{sql}: {actual:?} differs from no-FROM result {countstar_where_error:?}"
            ));
        }
        let desired = expected.clone().map_err(CoreError::Sql);
        if actual != desired {
            failures.push(format!("{sql}: execute {actual:?}, expected {desired:?}"));
        }
        match expected {
            Ok(_)
                if !sql.starts_with("CREATE TABLE")
                    && description != Ok(expected_types.clone()) =>
            {
                failures.push(format!(
                    "{sql}: describe {description:?}, expected {expected_types:?}"
                ))
            }
            Err(e) if description != Err(CoreError::Sql(e.clone())) => {
                failures.push(format!("{sql}: describe {description:?}, expected {e:?}"))
            }
            _ => {}
        }
        // Numeric equality ignores display scale; PostgreSQL's scale is also observable.
        if let Ok(rows) = &actual
            && let Ok(want) = &desired
        {
            for (row, expected_row) in rows.iter().zip(want) {
                for (value, expected_value) in row.iter().zip(expected_row) {
                    if let (V::Numeric(a), V::Numeric(b)) = (value, expected_value)
                        && a.to_string() != b.to_string()
                    {
                        failures.push(format!("{sql}: numeric scale {a}, expected {b}"));
                    }
                }
            }
        }
    }
    for (sql, table_free) in [
        ("SELECT count(*)", true),
        ("SELECT sum(1)", true),
        ("SELECT avg(DISTINCT 1)", true),
        ("SELECT abs(abs(avg(DISTINCT 1)))", true),
        ("SELECT 1 HAVING avg(DISTINCT 1)>0", true),
        ("SELECT public.avg(1)", false),
        ("SELECT public.count(DISTINCT 1)", false),
        ("SELECT position(max('b') IN 'abc')", true),
        ("SELECT count(nextval('so_id_seq'))", false),
    ] {
        let table = s.prepare(sql).map(|p| p.table().map(str::to_owned));
        let rejected = Prepared::rejects_unsupported_table_free_scalar(sql);
        if !matches!(&table, Ok(t) if t.is_none() == table_free) || rejected {
            failures.push(format!(
                "{sql}: routing table={table:?}, rejected={rejected}"
            ));
        }
    }
    for sql in ["SELECT public.avg(1)", "SELECT public.count(DISTINCT 1)"] {
        let actual = s.execute_at(sql, &[], None, &clock);
        if actual != Err(CoreError::Sql(E::UndefinedFunction)) {
            failures.push(format!("{sql}: execute {actual:?}, expected UndefinedFunction"));
        }
    }
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
