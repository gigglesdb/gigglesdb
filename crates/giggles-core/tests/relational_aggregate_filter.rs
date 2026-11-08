//! PostgreSQL 18.6 oracle: e3-query/wo-agg-c/pg-probes.txt (captured before these tests).
mod common;
use giggles_core::{
    relational::{DataType as T, Numeric, SqlError as E, SqlResult, Value as V},
    *,
};

fn database(test: impl FnOnce(&mut SqlSession)) {
    let path = common::fresh("aggregate-filter");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    run(
        &mut session,
        "CREATE TABLE t(id int PRIMARY KEY,g int,x int,b boolean)",
    );
    run(
        &mut session,
        "INSERT INTO t VALUES(1,1,10,true),(2,1,20,false),(3,2,20,NULL),(4,2,NULL,true),(5,2,30,true),(6,2,30,true)",
    );
    test(&mut session);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    run(s, sql).rows.into_iter().map(|r| r.values).collect()
}
fn n(text: &str) -> V {
    V::Numeric(Numeric::parse(text).unwrap())
}

#[test]
fn filtered_aggregates_global_grouped_distinct_having_and_order() {
    database(|s| {
        assert_eq!(
            rows(
                s,
                "SELECT count(*) FILTER (WHERE id>1),count(x) FILTER (WHERE b),sum(x) FILTER (WHERE b),avg(x) FILTER (WHERE b),min(x) FILTER (WHERE b),max(x) FILTER (WHERE b) FROM t"
            ),
            vec![vec![
                V::Int(5),
                V::Int(3),
                V::Int(70),
                n("23.3333333333333333"),
                V::Int(10),
                V::Int(30)
            ]]
        );
        assert_eq!(
            rows(
                s,
                "SELECT count(DISTINCT x) FILTER (WHERE b),sum(DISTINCT x) FILTER (WHERE b),avg(DISTINCT x) FILTER (WHERE b),min(DISTINCT x) FILTER (WHERE b),max(DISTINCT x) FILTER (WHERE b) FROM t"
            ),
            vec![vec![
                V::Int(2),
                V::Int(40),
                n("20.0000000000000000"),
                V::Int(10),
                V::Int(30)
            ]]
        );
        assert_eq!(
            rows(
                s,
                "SELECT g,count(*) FILTER (WHERE b),count(x) FILTER (WHERE b),sum(x) FILTER (WHERE b),avg(x) FILTER (WHERE b),min(x) FILTER (WHERE b),max(x) FILTER (WHERE b) FROM t GROUP BY g ORDER BY g"
            ),
            vec![
                vec![
                    V::Int(1),
                    V::Int(1),
                    V::Int(1),
                    V::Int(10),
                    n("10.0000000000000000"),
                    V::Int(10),
                    V::Int(10)
                ],
                vec![
                    V::Int(2),
                    V::Int(3),
                    V::Int(2),
                    V::Int(60),
                    n("30.0000000000000000"),
                    V::Int(30),
                    V::Int(30)
                ]
            ]
        );
        assert_eq!(
            rows(
                s,
                "SELECT g,count(DISTINCT x) FILTER (WHERE b),sum(DISTINCT x) FILTER (WHERE b),avg(DISTINCT x) FILTER (WHERE b),min(DISTINCT x) FILTER (WHERE b),max(DISTINCT x) FILTER (WHERE b) FROM t GROUP BY g HAVING count(*) FILTER (WHERE b)>0 ORDER BY sum(DISTINCT x) FILTER (WHERE b) DESC"
            ),
            vec![
                vec![
                    V::Int(2),
                    V::Int(1),
                    V::Int(30),
                    n("30.0000000000000000"),
                    V::Int(30),
                    V::Int(30)
                ],
                vec![
                    V::Int(1),
                    V::Int(1),
                    V::Int(10),
                    n("10.0000000000000000"),
                    V::Int(10),
                    V::Int(10)
                ]
            ]
        );
        assert_eq!(
            rows(
                s,
                "SELECT count(*) FILTER (WHERE id>1) FROM t HAVING count(*) FILTER (WHERE b)>0 ORDER BY sum(x) FILTER (WHERE b)"
            ),
            vec![vec![V::Int(5)]]
        );
    });
}

#[test]
fn false_null_empty_filters_and_argument_short_circuit() {
    database(|s| {
        assert_eq!(
            rows(
                s,
                "SELECT count(*) FILTER (WHERE b),count(*) FILTER (WHERE NOT b),count(*) FILTER (WHERE b IS NULL) FROM t"
            ),
            vec![vec![V::Int(4), V::Int(1), V::Int(1)]]
        );
        assert_eq!(
            rows(
                s,
                "SELECT sum(1/(id-id)) FILTER (WHERE false),sum(DISTINCT 1/(id-id)) FILTER (WHERE false),count(*) FILTER (WHERE NULL) FROM t"
            ),
            vec![vec![V::Null, V::Null, V::Int(0)]]
        );
        assert_eq!(
            rows(s, "SELECT sum(10/(id-1)) FILTER (WHERE id<>1) FROM t"),
            vec![vec![V::Int(22)]]
        );
        for predicate in ["false", "NULL", "id<0"] {
            assert_eq!(
                rows(
                    s,
                    &format!(
                        "SELECT count(*) FILTER (WHERE {predicate}),count(x) FILTER (WHERE {predicate}),sum(x) FILTER (WHERE {predicate}),avg(x) FILTER (WHERE {predicate}),min(x) FILTER (WHERE {predicate}),max(x) FILTER (WHERE {predicate}) FROM t"
                    )
                ),
                vec![vec![
                    V::Int(0),
                    V::Int(0),
                    V::Null,
                    V::Null,
                    V::Null,
                    V::Null
                ]]
            );
        }
        assert_eq!(
            rows(
                s,
                "SELECT count(*) FILTER (WHERE b),sum(x) FILTER (WHERE b) FROM t WHERE id<0"
            ),
            vec![vec![V::Int(0), V::Null]]
        );
        assert_eq!(
            rows(
                s,
                "SELECT g,count(*) FILTER (WHERE false) FROM t GROUP BY g ORDER BY g"
            ),
            vec![vec![V::Int(1), V::Int(0)], vec![V::Int(2), V::Int(0)]]
        );
        assert!(
            rows(
                s,
                "SELECT g,count(*) FILTER (WHERE b) FROM t WHERE id<0 GROUP BY g"
            )
            .is_empty()
        );
    });
}

#[test]
fn filter_validation_including_nested_scalar_expressions() {
    database(|s| {
        for (sql, error) in [
            (
                "SELECT sum(count(*) FILTER (WHERE b)) FILTER (WHERE true) FROM t",
                E::GroupingError,
            ),
            (
                "SELECT count(*) FILTER (WHERE count(*)>0) FROM t",
                E::GroupingError,
            ),
            (
                "SELECT count(*) FILTER (WHERE sum(x)>0) FROM t",
                E::GroupingError,
            ),
            (
                "SELECT count(*) FILTER (WHERE count(*) FILTER (WHERE b)>0) FROM t",
                E::GroupingError,
            ),
            (
                "SELECT count(*) FILTER (WHERE CASE WHEN sum(x)>0 THEN true ELSE false END) FROM t",
                E::GroupingError,
            ),
            (
                "SELECT sum(CASE WHEN true THEN count(*) FILTER (WHERE b) ELSE 0 END) FROM t",
                E::GroupingError,
            ),
            (
                "SELECT count(count(*) FILTER (WHERE b)>0) FROM t",
                E::GroupingError,
            ),
            ("SELECT sum(x) FILTER (WHERE 1) FROM t", E::Type),
            ("SELECT sum(x) FILTER (WHERE '1'::text) FROM t", E::Type),
            (
                "SELECT count(*) FILTER (WHERE 'bad') FROM t",
                E::InvalidTextRepresentation,
            ),
            (
                "SELECT count(*) FILTER (WHERE 'true'::text) FROM t",
                E::Type,
            ),
            (
                "SELECT count(*) FILTER (WHERE CASE WHEN true THEN 'true' ELSE 'false' END) FROM t",
                E::Type,
            ),
            ("SELECT abs(x) FILTER (WHERE 1) FROM t", E::Type),
            ("SELECT nosuch(x) FILTER (WHERE 1) FROM t", E::Type),
            ("SELECT sum(count(x)) FILTER (WHERE 1) FROM t", E::Type),
            (
                "SELECT sum(count(x) FILTER (WHERE 1)) FILTER (WHERE true) FROM t",
                E::Type,
            ),
            (
                "SELECT sum(count(x) FILTER (WHERE 1)) FILTER (WHERE false) FROM t",
                E::Type,
            ),
            (
                "SELECT sum(count(x) FILTER (WHERE true)) FILTER (WHERE 1) FROM t",
                E::Type,
            ),
            (
                "SELECT sum(nosuch(count(x))) FILTER (WHERE 1) FROM t",
                E::UndefinedFunction,
            ),
            (
                "SELECT sum(abs(count(x),x)) FILTER (WHERE 1) FROM t",
                E::UndefinedFunction,
            ),
            (
                "SELECT sum(CASE WHEN count(x) THEN 1 ELSE 0 END) FILTER (WHERE 1) FROM t",
                E::Type,
            ),
            (
                "SELECT sum(missing) FILTER (WHERE 1) FROM t",
                E::UnknownColumn,
            ),
            (
                "SELECT abs(missing) FILTER (WHERE 1) FROM t",
                E::UnknownColumn,
            ),
            (
                "SELECT sum(nosuch(x)) FILTER (WHERE 1) FROM t",
                E::UndefinedFunction,
            ),
            (
                "SELECT sum(count(missing)) FILTER (WHERE 1) FROM t",
                E::UnknownColumn,
            ),
            ("SELECT abs(x) FILTER (WHERE b) FROM t", E::WrongObjectType),
            (
                "SELECT abs(x) FILTER (WHERE nosuch(b)) FROM t",
                E::UndefinedFunction,
            ),
            (
                "SELECT abs(x) FILTER (WHERE sum(x)>0) FROM t",
                E::GroupingError,
            ),
            (
                "SELECT x FROM t ORDER BY abs(x) FILTER (WHERE b)",
                E::WrongObjectType,
            ),
            (
                "SELECT x FROM t HAVING abs(x) FILTER (WHERE b)>0",
                E::WrongObjectType,
            ),
            (
                "SELECT id FROM t WHERE abs(x) FILTER (WHERE b)>0",
                E::WrongObjectType,
            ),
            (
                "SELECT id FROM t WHERE nosuch(x) FILTER (WHERE b)>0",
                E::UndefinedFunction,
            ),
            (
                "SELECT id FROM t WHERE count(*) FILTER (WHERE 1)>0",
                E::Type,
            ),
            (
                "SELECT id FROM t WHERE count(*) FILTER (WHERE true)>0",
                E::GroupingError,
            ),
            (
                "SELECT id FROM t GROUP BY abs(x) FILTER (WHERE b),id",
                E::WrongObjectType,
            ),
            (
                "SELECT id FROM t GROUP BY count(*) FILTER (WHERE 1),id",
                E::Type,
            ),
            (
                "SELECT id FROM t GROUP BY count(*) FILTER (WHERE true),id",
                E::GroupingError,
            ),
            ("SELECT x FROM t ORDER BY sum(x) FILTER (WHERE 1)", E::Type),
            (
                "SELECT nosuch(x) FILTER (WHERE b) FROM t",
                E::UndefinedFunction,
            ),
            (
                "SELECT count(*) FILTER (WHERE nosuch(b)) FROM t",
                E::UndefinedFunction,
            ),
            ("SELECT abs(1) FILTER (WHERE true)", E::WrongObjectType),
            ("SELECT sum(1) FILTER (WHERE 1)", E::Type),
            ("SELECT sum(1) FILTER (WHERE '1'::text)", E::Type),
            (
                "SELECT public.count(*) FILTER (WHERE true)",
                E::UndefinedFunction,
            ),
        ] {
            assert_eq!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(error)),
                "{sql}"
            );
        }
    });
}

#[test]
fn filtered_aggregates_inside_predicates_cases_and_without_from() {
    database(|s| {
        for (sql, expected) in [
            (
                "SELECT count(*),sum(1),count(*) FILTER (WHERE true),count(*) FILTER (WHERE false),sum(1) FILTER (WHERE NULL),pg_catalog.count(*) FILTER (WHERE true)",
                vec![vec![
                    V::Int(1),
                    V::Int(1),
                    V::Int(1),
                    V::Int(0),
                    V::Null,
                    V::Int(1),
                ]],
            ),
            (
                "SELECT count(*) FILTER (WHERE b)>2,CASE WHEN count(*) FILTER (WHERE b)>2 THEN sum(x) FILTER (WHERE b) ELSE 0 END FROM t",
                vec![vec![V::Bool(true), V::Int(70)]],
            ),
            (
                "SELECT CASE WHEN true THEN count(*) FILTER (WHERE b) ELSE 0 END FROM t GROUP BY g ORDER BY g",
                vec![vec![V::Int(1)], vec![V::Int(3)]],
            ),
            (
                "SELECT CASE WHEN count(*) FILTER (WHERE true)>0 THEN sum(2) FILTER (WHERE true) ELSE 0 END,count(*) FILTER (WHERE false)>0",
                vec![vec![V::Int(2), V::Bool(false)]],
            ),
            (
                "SELECT count(*) FILTER (WHERE CASE WHEN id>2 THEN b ELSE false END),count(*) FILTER (WHERE b IS TRUE),count(*) FILTER (WHERE b IS NOT FALSE),count(*) FILTER (WHERE b IS UNKNOWN) FROM t",
                vec![vec![V::Int(3), V::Int(4), V::Int(5), V::Int(1)]],
            ),
            (
                "SELECT count(*) FILTER (WHERE b) AS c,count(*) FILTER (WHERE t.b) AS c FROM t ORDER BY c",
                vec![vec![V::Int(4), V::Int(4)]],
            ),
            (
                "SELECT count(*) FILTER (WHERE 'true'),count(*) FILTER (WHERE 'false'),count(*) FILTER (WHERE 'true' AND b),count(*) FILTER (WHERE CASE WHEN 'true' THEN b ELSE false END) FROM t",
                vec![vec![V::Int(6), V::Int(0), V::Int(4), V::Int(4)]],
            ),
        ] {
            assert_eq!(rows(s, sql), expected, "{sql}");
        }
    });
}

#[test]
fn undefined_filter_routines_never_get_a_table_free_proof() {
    database(|s| {
        for sql in [
            "SELECT CASE WHEN false THEN count(*) FILTER (WHERE nosuch()) ELSE 0 END",
            "SELECT count(*) FILTER (WHERE nosuch())>0",
            "SELECT count(*) FILTER (WHERE CASE WHEN false THEN nosuch() ELSE true END)",
        ] {
            // Even an unreachable routine must keep the deferred routing target.
            let p = s.prepare(sql).unwrap();
            assert!(p.table().is_some(), "{sql}");
            assert_eq!(
                s.describe(&p).unwrap_err(),
                CoreError::Sql(E::UndefinedFunction),
                "{sql}"
            );
            assert_eq!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(E::UndefinedFunction)),
                "{sql}"
            );
        }
    });
}

#[test]
fn filter_evaluates_volatile_and_clock_predicates_per_input_row() {
    database(|s| {
        run(s, "CREATE SEQUENCE filter_calls");
        let clock = giggles_core::relational::StatementClock {
            unavailable: None,
            transaction_us: 100,
            statement_us: 100,
            zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
            wall: None,
        };
        let mut clock_rows = |sql: &str| {
            s.execute_at(sql, &[], None, &clock)
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            clock_rows("SELECT count(*) FILTER (WHERE nextval('filter_calls')>0) FROM t"),
            vec![vec![V::Int(6)]],
        );
        assert_eq!(
            clock_rows("SELECT nextval('filter_calls')"),
            vec![vec![V::Int(7)]]
        );
        assert_eq!(
            clock_rows("SELECT count(*) FILTER (WHERE current_timestamp=current_timestamp) FROM t"),
            vec![vec![V::Int(6)]],
        );
    });
}

#[test]
fn filter_parameters_describe_casts_and_boolean_expressions() {
    database(|s| {
        for (sql, error) in [
            (
                "SELECT count($1) FILTER (WHERE $1) FROM t",
                E::AmbiguousParameter,
            ),
            (
                "SELECT count($1) FILTER (WHERE $1>1) FROM t",
                E::AmbiguousParameter,
            ),
            (
                "SELECT sum($1) FILTER (WHERE $1) FROM t",
                E::AmbiguousFunction,
            ),
            (
                "SELECT avg($1) FILTER (WHERE $1>1) FROM t",
                E::AmbiguousFunction,
            ),
            (
                "SELECT min($1) FILTER (WHERE $1) FROM t",
                E::AmbiguousParameter,
            ),
            (
                "SELECT max($1) FILTER (WHERE $1) FROM t",
                E::AmbiguousParameter,
            ),
            (
                "SELECT min($1) FILTER (WHERE $1>1) FROM t",
                E::AmbiguousParameter,
            ),
            (
                "SELECT min($1) FILTER (WHERE $1 IS NULL) FROM t",
                E::AmbiguousParameter,
            ),
        ] {
            let p = s.prepare(sql).unwrap();
            assert_eq!(s.describe(&p).unwrap_err(), CoreError::Sql(error), "{sql}");
        }
        for sql in [
            "SELECT min($1) FILTER (WHERE $1::text<>'') FROM t",
            "SELECT min($1) FILTER (WHERE $1='a') FROM t",
            "SELECT min($1::text) FILTER (WHERE $1 IS NULL) FROM t",
        ] {
            let p = s.prepare(sql).unwrap();
            assert_eq!(s.describe(&p).unwrap().parameters, vec![T::Text], "{sql}");
        }
        for (sql, types, params, expected) in [
            (
                "SELECT count(*) FILTER (WHERE $1) FROM t",
                vec![T::Boolean],
                vec![V::Bool(true)],
                vec![vec![V::Int(6)]],
            ),
            (
                "SELECT sum(x) FILTER (WHERE x>$1 AND $2) FROM t",
                vec![T::Integer, T::Boolean],
                vec![V::Int(10), V::Bool(true)],
                vec![vec![V::Int(100)]],
            ),
            (
                "SELECT g FROM t GROUP BY g HAVING count(*) FILTER (WHERE $1)>0 ORDER BY sum(x) FILTER (WHERE id>$2)",
                vec![T::Boolean, T::Integer],
                vec![V::Bool(true), V::Int(2)],
                vec![vec![V::Int(2)], vec![V::Int(1)]],
            ),
            (
                "SELECT count(*) FILTER (WHERE $1)>$2,CASE WHEN $3 THEN sum(x) FILTER (WHERE x>$4) ELSE 0 END FROM t",
                vec![T::Boolean, T::BigInt, T::Boolean, T::Integer],
                vec![V::Bool(true), V::Int(2), V::Bool(true), V::Int(10)],
                vec![vec![V::Bool(true), V::Int(100)]],
            ),
            (
                "SELECT CASE WHEN count(*) FILTER (WHERE $1)>$2 THEN sum($3::int) FILTER (WHERE $4) ELSE 0 END",
                vec![T::Boolean, T::BigInt, T::Integer, T::Boolean],
                vec![V::Bool(true), V::Int(0), V::Int(7), V::Bool(true)],
                vec![vec![V::Int(7)]],
            ),
        ] {
            let p = s.prepare(sql).unwrap();
            assert_eq!(s.describe(&p).unwrap().parameters, types, "{sql}");
            assert_eq!(
                s.execute_prepared(&p, &params, None)
                    .unwrap()
                    .rows
                    .into_iter()
                    .map(|r| r.values)
                    .collect::<Vec<_>>(),
                expected,
                "{sql}"
            );
        }
        let p=s.prepare("SELECT count(*) FILTER (WHERE b),count(x) FILTER (WHERE b),sum(x) FILTER (WHERE b),avg(x) FILTER (WHERE b),min(x) FILTER (WHERE b),max(x) FILTER (WHERE b) FROM t").unwrap();
        let d = s.describe(&p).unwrap();
        assert_eq!(
            d.columns
                .into_iter()
                .map(|c| c.data_type)
                .collect::<Vec<_>>(),
            vec![
                T::BigInt,
                T::BigInt,
                T::BigInt,
                T::Numeric,
                T::Integer,
                T::Integer
            ]
        );
        assert_eq!(d.typmods, vec![-1; 6]);
        assert_eq!(
            rows(
                s,
                "SELECT count(*) FILTER (WHERE x IN (10,30) AND id BETWEEN 1 AND 5) FROM t"
            ),
            vec![vec![V::Int(2)]]
        );
        assert_eq!(
            rows(
                s,
                "SELECT (count(*) FILTER (WHERE b))+1,pg_catalog.sum(x) FILTER (WHERE b),(count(*) FILTER (WHERE b))::int FROM t"
            ),
            vec![vec![V::Int(5), V::Int(70), V::Int(4)]]
        );
        assert_eq!(
            rows(
                s,
                "SELECT count(*) FILTER (WHERE CAST(b AS boolean) AND (id>1 OR id=1)) FROM t"
            ),
            vec![vec![V::Int(4)]]
        );
    });
}

#[test]
fn filtered_distinct_cancellation_and_input_limits() {
    database(|s| {
        run(s, "BEGIN");
        let cancel = Cancellation::default();
        cancel.cancel();
        assert_eq!(
            cancel.scope(|| s.execute(
                "SELECT g,avg(DISTINCT x) FILTER (WHERE b) FROM t GROUP BY g",
                &[],
                None
            )),
            Err(CoreError::Cancelled)
        );
        assert!(s.is_aborted());
        s.rollback();
        assert_eq!(
            rows(s, "SELECT count(DISTINCT x) FILTER (WHERE b) FROM t"),
            vec![vec![V::Int(2)]]
        );
        for start in (7..=1030).step_by(128) {
            let values = (start..start + 128)
                .map(|id| format!("({id},3,{id},true)"))
                .collect::<Vec<_>>()
                .join(",");
            run(s, &format!("INSERT INTO t VALUES {values}"));
        }
        // Predicate work totals more than MAX_RESULT_BYTES, but its budget is per row.
        assert_eq!(
            rows(
                s,
                "SELECT count(*) FILTER (WHERE length(repeat('x',16380))>0) FROM t"
            ),
            vec![vec![V::Int(1030)]]
        );
        // FILTER cannot evade the existing scalar input budget when admitted.
        let argument = "repeat(lpad(id::text,4,'0'),16380)";
        assert_eq!(
            s.execute(
                &format!("SELECT count(DISTINCT {argument}) FILTER (WHERE true) FROM t LIMIT 1"),
                &[],
                None
            ),
            Err(CoreError::Sql(E::Limit))
        );
        assert_eq!(
            rows(
                s,
                &format!("SELECT count(DISTINCT {argument}) FILTER (WHERE false) FROM t")
            ),
            vec![vec![V::Int(0)]]
        );
    });
}
