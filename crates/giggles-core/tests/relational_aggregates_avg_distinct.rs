//! PostgreSQL 18.6 oracle: e3-query/wo-agg-b/pg-probes.txt.
mod common;
use giggles_core::{
    relational::{DataType as T, Float8, Numeric, SqlError as E, SqlResult, Value as V},
    *,
};
fn database(test: impl FnOnce(&mut SqlSession)) {
    let path = common::fresh("avg-distinct");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
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
fn f(text: &str) -> V {
    V::Float8(Float8::parse(text).unwrap())
}
// Numeric typmod DDL is outside the current parser. Preserve exactly the
// values/scales delivered by PG numeric(10,3) columns (1.500 and 2.250).
fn fixture(s: &mut SqlSession) {
    run(
        s,
        "CREATE TABLE t(id int PRIMARY KEY,k int,v int,n numeric,f float8)",
    );
    run(
        s,
        "INSERT INTO t VALUES(1,1,1,1.500,1),(2,1,1,1.500,1),(3,1,2,2.250,2),(4,2,4,NULL,4),(5,2,NULL,NULL,NULL)",
    );
}
#[test]
fn avg_types_typmods_rounding_and_empty_sets() {
    database(|s| {
        fixture(s);
        let sql = "SELECT avg(v::int2),avg(v),avg(v::int8),avg(n),avg(f::float4),avg(f) FROM t";
        let p = s.prepare(sql).unwrap();
        let d = s.describe(&p).unwrap();
        assert_eq!(
            d.columns
                .into_iter()
                .map(|c| c.data_type)
                .collect::<Vec<_>>(),
            vec![
                T::Numeric,
                T::Numeric,
                T::Numeric,
                T::Numeric,
                T::DoublePrecision,
                T::DoublePrecision
            ]
        );
        assert_eq!(d.typmods, vec![-1; 6]);
        assert_eq!(
            rows(
                s,
                "SELECT avg(v),avg(v::int8),avg(v::numeric) FROM t WHERE id<>2"
            ),
            vec![vec![n("2.3333333333333333"); 3]]
        );
        let pair = rows(s, "SELECT avg(n) FROM t WHERE id IN (1,3)");
        assert_eq!(pair, vec![vec![n("1.8750000000000000")]]);
        if let V::Numeric(value) = &pair[0][0] {
            assert_eq!(value.to_string(), "1.8750000000000000");
        }
        // The single-value scale is an observable PG rounding rule.
        let one = rows(s, "SELECT avg(n) FROM t WHERE id=1");
        assert_eq!(one, vec![vec![n("1.50000000000000000000")]]);
        if let V::Numeric(value) = &one[0][0] {
            assert_eq!(value.to_string(), "1.50000000000000000000");
        }
        assert_eq!(
            rows(
                s,
                "SELECT avg(v),avg(n),avg(f),count(DISTINCT v),sum(DISTINCT v),avg(DISTINCT v),min(DISTINCT v),max(DISTINCT v) FROM t WHERE id<0"
            ),
            vec![vec![
                V::Null,
                V::Null,
                V::Null,
                V::Int(0),
                V::Null,
                V::Null,
                V::Null,
                V::Null
            ]]
        );
        assert!(rows(s, "SELECT k,avg(DISTINCT v) FROM t WHERE id<0 GROUP BY k").is_empty());
    });
}
#[test]
fn distinct_is_per_group_per_aggregate_and_works_in_having_order() {
    database(|s| {
        fixture(s);
        assert_eq!(
            rows(
                s,
                "SELECT k,count(v),count(DISTINCT v),sum(v),sum(DISTINCT v),avg(v),avg(DISTINCT v),min(DISTINCT v),max(DISTINCT v) FROM t GROUP BY k ORDER BY k"
            ),
            vec![
                vec![
                    V::Int(1),
                    V::Int(3),
                    V::Int(2),
                    V::Int(4),
                    V::Int(3),
                    n("1.3333333333333333"),
                    n("1.5000000000000000"),
                    V::Int(1),
                    V::Int(2)
                ],
                vec![
                    V::Int(2),
                    V::Int(1),
                    V::Int(1),
                    V::Int(4),
                    V::Int(4),
                    n("4.0000000000000000"),
                    n("4.0000000000000000"),
                    V::Int(4),
                    V::Int(4)
                ]
            ]
        );
        assert_eq!(
            rows(
                s,
                "SELECT k FROM t GROUP BY k HAVING count(DISTINCT v)>0 ORDER BY avg(DISTINCT v) DESC"
            ),
            vec![vec![V::Int(2)], vec![V::Int(1)]]
        );
        let p=s.prepare("SELECT k,avg(DISTINCT v) FROM t WHERE id>$1 GROUP BY k HAVING count(DISTINCT v)>$2 ORDER BY avg(DISTINCT v)").unwrap();
        assert_eq!(
            s.describe(&p).unwrap().parameters,
            vec![T::Integer, T::BigInt]
        );
        assert_eq!(
            s.execute_prepared(&p, &[V::Int(0), V::Int(1)], None)
                .unwrap()
                .rows[0]
                .values,
            vec![V::Int(1), n("1.5000000000000000")]
        );
    });
}
#[test]
fn float_specials_overflow_and_distinct_equality() {
    database(|s| {
        run(s, "CREATE TABLE t(id int PRIMARY KEY,f float8,n numeric)");
        run(
            s,
            "INSERT INTO t VALUES(1,'Infinity',1.0),(2,'Infinity',1.00)",
        );
        assert_eq!(
            rows(s, "SELECT avg(f),count(DISTINCT n),sum(DISTINCT n) FROM t"),
            vec![vec![f("Infinity"), V::Int(1), n("1.0")]]
        );
        run(s, "UPDATE t SET f='-Infinity'::float8 WHERE id=2");
        assert_eq!(rows(s, "SELECT avg(f) FROM t"), vec![vec![f("NaN")]]);
        run(s, "UPDATE t SET f='NaN'::float8 WHERE id=1");
        assert_eq!(rows(s, "SELECT avg(f) FROM t"), vec![vec![f("NaN")]]);
        run(s, "UPDATE t SET f=1e308::float8");
        for sql in ["SELECT sum(f) FROM t", "SELECT avg(f) FROM t"] {
            assert_eq!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(E::Overflow)),
                "{sql}"
            );
        }
        run(s, "UPDATE t SET f='0'::float8 WHERE id=1");
        run(s, "UPDATE t SET f='-0'::float8 WHERE id=2");
        run(s, "INSERT INTO t VALUES(3,'NaN',2),(4,'NaN',2.00)");
        assert_eq!(
            rows(
                s,
                "SELECT count(DISTINCT f),sum(DISTINCT f),avg(DISTINCT f),count(DISTINCT n) FROM t"
            ),
            vec![vec![V::Int(2), f("NaN"), f("NaN"), V::Int(2)]]
        );
        assert_eq!(
            rows(s, "SELECT f,count(*) FROM t GROUP BY f ORDER BY f"),
            vec![vec![f("0"), V::Int(2)], vec![f("NaN"), V::Int(2)]]
        );
        assert_eq!(
            rows(s, "SELECT n,count(*) FROM t GROUP BY n ORDER BY n"),
            vec![vec![n("1.0"), V::Int(2)], vec![n("2"), V::Int(2)]]
        );
    });
}
#[test]
fn invalid_aggregate_signatures_and_grouping_are_rejected() {
    database(|s| {
        fixture(s);
        for sql in [
            "SELECT count(DISTINCT *) FROM t",
            "SELECT avg(DISTINCT) FROM t",
        ] {
            assert_eq!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(E::Syntax)),
                "{sql}"
            );
        }
        // Interval avg exists in PG18 but is deliberately outside this project's slice.
        for sql in [
            "SELECT avg(v::text) FROM t",
            "SELECT avg(true) FROM t",
            "SELECT avg(interval '1 day') FROM t",
            "SELECT public.avg(v) FROM t",
            "SELECT public.avg(DISTINCT v) FROM t",
        ] {
            assert_eq!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(E::UndefinedFunction)),
                "{sql}"
            );
        }
        for sql in [
            "SELECT k,avg(v) FROM t",
            "SELECT avg(sum(v)) FROM t",
            "SELECT k FROM t WHERE avg(v)>0",
        ] {
            assert_eq!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(E::GroupingError)),
                "{sql}"
            );
        }
    });
}
#[test]
fn cancellation_aborts_avg_distinct_and_rollback_recovers() {
    database(|s| {
        fixture(s);
        run(s, "BEGIN");
        let cancel = Cancellation::default();
        cancel.cancel();
        assert_eq!(
            cancel.scope(|| s.execute("SELECT k,avg(DISTINCT v) FROM t GROUP BY k", &[], None)),
            Err(CoreError::Cancelled)
        );
        assert!(s.is_aborted());
        assert_eq!(
            s.execute("SELECT avg(v) FROM t", &[], None),
            Err(CoreError::Sql(E::Aborted))
        );
        s.rollback();
        assert_eq!(
            rows(s, "SELECT count(DISTINCT v) FROM t"),
            vec![vec![V::Int(3)]]
        );
    });
}
#[test]
// This exercises scalar argument limits; the engine Retention unit tests
// separately measure DISTINCT retained-state accounting and its cap.
fn oversized_distinct_argument_is_bounded_before_limit() {
    database(|s| {
        run(s, "CREATE TABLE t(id int PRIMARY KEY)");
        for start in (1..=1024).step_by(128) {
            let values = (start..start + 128)
                .map(|id| format!("({id})"))
                .collect::<Vec<_>>()
                .join(",");
            run(s, &format!("INSERT INTO t VALUES {values}"));
        }
        assert_eq!(
            s.execute(
                "SELECT count(DISTINCT repeat(lpad(id::text,4,'0'),16380)) FROM t LIMIT 1",
                &[],
                None
            ),
            Err(CoreError::Sql(E::Limit))
        );
        assert_eq!(
            rows(s, "SELECT count(DISTINCT id) FROM t"),
            vec![vec![V::Int(1024)]]
        );
    });
}
#[test]
fn requested_compat_corpus_executes() {
    for source in [
        include_str!("../../../compat/corpus/aggregates/avg_numeric_and_float.sql"),
        include_str!("../../../compat/corpus/aggregates/count_distinct.sql"),
        include_str!("../../../compat/corpus/aggregates/sum_avg_distinct_grouped.sql"),
    ] {
        database(|s| {
            let sql = source
                .lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            for q in sql.split(';').map(str::trim).filter(|q| !q.is_empty()) {
                run(s, q);
            }
        });
    }
}
#[test]
fn aggregate_arguments_and_sequence_calls_keep_analysis_boundaries() {
    database(|s| {
        fixture(s);
        assert_eq!(
            rows(
                s,
                "SELECT k,avg(DISTINCT abs(v))+1 FROM t GROUP BY k HAVING avg(DISTINCT abs(v))>0 ORDER BY avg(DISTINCT abs(v)) DESC"
            ),
            vec![
                vec![V::Int(2), n("5.0000000000000000")],
                vec![V::Int(1), n("2.5000000000000000")]
            ]
        );
        run(s, "CREATE TABLE sequence_owner(id serial PRIMARY KEY)");
        for sql in [
            "SELECT avg(nextval('sequence_owner_id_seq'))",
            "SELECT count(DISTINCT nextval('sequence_owner_id_seq'))",
            "SELECT avg(DISTINCT nextval('sequence_owner_id_seq'))",
        ] {
            let p = s.prepare(sql).unwrap();
            assert!(
                p.table().is_some(),
                "sequence authority must not get a table-free proof: {sql}"
            );
        }
    });
}

#[test]
fn unknown_and_known_unsupported_aggregates_keep_error_split() {
    database(|s| {
        fixture(s);
        assert_eq!(
            s.execute("SELECT made_up_agg(1)", &[], None),
            Err(CoreError::Sql(E::UndefinedFunction))
        );
        assert_eq!(
            s.execute("SELECT stddev(v) FROM t", &[], None),
            Err(CoreError::Sql(E::Unsupported))
        );
        assert_eq!(
            rows(s, "SELECT array_agg(v) FROM t"),
            vec![vec![V::Text("{1,1,2,4,NULL}".into())]]
        );
    });
}

#[test]
fn table_free_aggregates_keep_describe_and_execute_boundaries() {
    database(|s| {
        fixture(s);
        // PG18.6: aggregates evaluate the implicit row without table authority (DD-0068 F11).
        for (sql, data_type, expected) in [
            ("SELECT avg(1)", T::Numeric, n("1.00000000000000000000")),
            ("SELECT count(DISTINCT 1)", T::BigInt, V::Int(1)),
            ("SELECT avg(DISTINCT 1)", T::Numeric, n("1.00000000000000000000")),
            ("SELECT max(position('b' IN 'abc'))", T::Integer, V::Int(2)),
            ("SELECT position(max('b') IN 'abc')", T::Integer, V::Int(2)),
            ("SELECT abs(avg(1))", T::Numeric, n("1.00000000000000000000")),
            ("SELECT abs(abs(avg(DISTINCT 1)))", T::Numeric, n("1.00000000000000000000")),
            ("SELECT CAST(avg(DISTINCT 1) AS numeric)", T::Numeric, n("1.00000000000000000000")),
            ("SELECT 1 HAVING avg(DISTINCT 1)>0", T::Integer, V::Int(1)),
            ("SELECT 1 ORDER BY avg(DISTINCT 1)", T::Integer, V::Int(1)),
            ("SELECT avg(DISTINCT 1) ORDER BY 1", T::Numeric, n("1.00000000000000000000")),
            ("SELECT avg(1) HAVING true", T::Numeric, n("1.00000000000000000000")),
        ] {
            let p = s.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            assert!(p.table().is_none(), "{sql}");
            assert!(
                !giggles_core::relational::Prepared::rejects_unsupported_table_free_scalar(sql),
                "{sql}"
            );
            assert_eq!(s.describe(&p).unwrap().columns[0].data_type, data_type, "{sql}");
            let result = s.execute_prepared(&p, &[], None).unwrap();
            if let V::Numeric(value) = &result.rows[0].values[0] {
                assert_eq!(value.to_string(), "1.00000000000000000000", "{sql}");
            }
            assert_eq!(result.rows[0].values, vec![expected], "{sql}");
        }
        // PG18.6: GROUP BY cannot name an aggregate output (DD-0068 F11).
        let p = s.prepare("SELECT avg(1) GROUP BY 1").unwrap();
        assert_eq!(s.describe(&p).unwrap_err(), CoreError::Sql(E::GroupingError));
        assert_eq!(
            s.execute_prepared(&p, &[], None).unwrap_err(),
            CoreError::Sql(E::GroupingError)
        );
        assert_eq!(
            rows(s, "SELECT abs(abs(avg(DISTINCT v))) FROM t"),
            vec![vec![n("2.3333333333333333")]]
        );
        assert_eq!(
            rows(s, "SELECT CAST(avg(DISTINCT v) AS numeric) FROM t HAVING avg(DISTINCT v)>0 ORDER BY avg(DISTINCT v)"),
            vec![vec![n("2.3333333333333333")]]
        );
    });
}
