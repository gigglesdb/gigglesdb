mod common;

use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{SqlResult, Value},
};

const SETUP: &[&str] = &[
    "CREATE TABLE t(a int, b text, v int)",
    "INSERT INTO t VALUES (1,'x',10),(1,'y',20),(3,'x',30),(3,NULL,40),(NULL,'y',50)",
    "CREATE TABLE e(a int)",
];

// PostgreSQL 18.6. GROUPING breaks peer ties where they can change projected rows.
const CASES: &[(&str, &str)] = &[
    (
        "SELECT a, count(*), count(*) OVER () FROM t GROUP BY a ORDER BY a",
        "1,2,3;3,2,3;NULL,1,3",
    ),
    (
        "SELECT a, count(*) OVER () FROM t GROUP BY ROLLUP(a) ORDER BY a",
        "1,4;3,4;NULL,4;NULL,4",
    ),
    (
        "SELECT a, b, sum(v), row_number() OVER (ORDER BY a, b, grouping(a,b)) FROM t GROUP BY \
         ROLLUP(a, b) ORDER BY a, b, grouping(a,b)",
        "1,x,10,1;1,y,20,2;1,NULL,30,3;3,x,30,4;3,NULL,40,5;3,NULL,70,6;NULL,y,50,7;NULL,NULL,50,8;NULL,NULL,150,9",
    ),
    (
        "SELECT a, b, sum(v) AS s, grouping(a, b) AS g, rank() OVER (PARTITION BY grouping(a, b) \
         ORDER BY sum(v) DESC) FROM t GROUP BY ROLLUP(a, b) ORDER BY g, a, b",
        "1,x,10,0,5;1,y,20,0,4;3,x,30,0,3;3,NULL,40,0,2;NULL,y,50,0,1;1,NULL,30,1,3;3,NULL,70,1,1;NULL,NULL,50,1,2;NULL,NULL,150,3,1",
    ),
    (
        "SELECT a, b, sum(v) AS s, sum(sum(v)) OVER (ORDER BY grouping(a,b), a, b) FROM t GROUP \
         BY CUBE(a, b) ORDER BY grouping(a,b), a, b",
        "1,x,10,10;1,y,20,30;3,x,30,60;3,NULL,40,100;NULL,y,50,150;1,NULL,30,180;3,NULL,70,250;NULL,NULL,50,300;NULL,x,40,340;NULL,y,70,410;NULL,NULL,40,450;NULL,NULL,150,600",
    ),
    (
        "SELECT a, b, count(*) AS c, lag(count(*)) OVER (ORDER BY a NULLS FIRST, b NULLS FIRST, \
         grouping(a,b)) FROM t GROUP BY GROUPING SETS ((a), (b), ()) ORDER BY a NULLS FIRST, b \
         NULLS FIRST, grouping(a,b)",
        "NULL,NULL,1,NULL;NULL,NULL,1,1;NULL,NULL,5,1;NULL,x,2,5;NULL,y,2,2;1,NULL,2,2;3,NULL,2,2",
    ),
    (
        "SELECT a, sum(v), sum(sum(v)) OVER (PARTITION BY a IS NULL) FROM t GROUP BY ROLLUP(a) \
         ORDER BY 1, 2",
        "1,30,100;3,70,100;NULL,50,200;NULL,150,200",
    ),
    (
        "SELECT a, grouping(a), count(*), row_number() OVER (ORDER BY grouping(a) DESC, a) FROM \
         t GROUP BY ROLLUP(a) ORDER BY grouping(a) DESC, a",
        "NULL,1,5,1;1,0,2,2;3,0,2,3;NULL,0,1,4",
    ),
    (
        "SELECT a, b, count(*), count(*) OVER () FROM t GROUP BY CUBE(a, b) HAVING count(*) > 1 \
         ORDER BY a, b",
        "1,NULL,2,5;3,NULL,2,5;NULL,x,2,5;NULL,y,2,5;NULL,NULL,5,5",
    ),
    (
        "SELECT count(*), sum(count(*)) OVER () FROM t GROUP BY GROUPING SETS (())",
        "5,5",
    ),
    (
        "SELECT DISTINCT count(*) OVER () FROM t GROUP BY ROLLUP(a)",
        "4",
    ),
    (
        "SELECT a, row_number() OVER (ORDER BY a DESC NULLS LAST) FROM t GROUP BY ROLLUP(a) \
         ORDER BY 2 LIMIT 2",
        "3,1;1,2",
    ),
    (
        "SELECT a, b, sum(sum(v)) OVER w FROM t GROUP BY ROLLUP(a, b) WINDOW w AS (PARTITION BY \
         a ORDER BY b) ORDER BY a, b",
        "1,x,10;1,y,30;1,NULL,60;3,x,30;3,NULL,140;3,NULL,140;NULL,y,50;NULL,NULL,250;NULL,NULL,250",
    ),
    (
        "SELECT a, count(*), count(*) OVER () FROM e GROUP BY ROLLUP(a)",
        "NULL,0,1",
    ),
    (
        "SELECT a, count(*), count(*) OVER () FROM e GROUP BY CUBE(a)",
        "NULL,0,1",
    ),
    (
        "SELECT count(*), count(*) OVER () FROM e GROUP BY GROUPING SETS ((), ())",
        "0,2;0,2",
    ),
    (
        "SELECT a, b, count(*), dense_rank() OVER (ORDER BY count(*)) FROM t GROUP BY a, \
         ROLLUP(b) ORDER BY a, b, grouping(a,b)",
        "1,x,1,1;1,y,1,1;1,NULL,2,2;3,x,1,1;3,NULL,1,1;3,NULL,2,2;NULL,y,1,1;NULL,NULL,1,1",
    ),
    (
        "SELECT * FROM (SELECT a, count(*) OVER () AS w FROM t GROUP BY ROLLUP(a)) s ORDER BY a",
        "1,4;3,4;NULL,4;NULL,4",
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY DISTINCT ROLLUP(a, b), ROLLUP(a) ORDER BY a, b, \
         grouping(a,b)",
        "1,x,1;1,y,1;1,NULL,2;3,x,1;3,NULL,1;3,NULL,2;NULL,y,1;NULL,NULL,1;NULL,NULL,5",
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY ALL ROLLUP(a, b), ROLLUP(a) ORDER BY a, b, \
         grouping(a,b)",
        "1,x,1;1,x,1;1,y,1;1,y,1;1,NULL,2;1,NULL,2;1,NULL,2;3,x,1;3,x,1;3,NULL,1;3,NULL,1;3,NULL,2;3,NULL,2;3,NULL,2;NULL,y,1;NULL,y,1;NULL,NULL,1;NULL,NULL,1;NULL,NULL,1;NULL,NULL,5",
    ),
    (
        "SELECT a, count(*) FROM t GROUP BY DISTINCT a ORDER BY a",
        "1,2;3,2;NULL,1",
    ),
    (
        "SELECT a, count(*) FROM t GROUP BY ALL a ORDER BY a",
        "1,2;3,2;NULL,1",
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY (a, b) ORDER BY a, b",
        "1,x,1;1,y,1;3,x,1;3,NULL,1;NULL,y,1",
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY (a), (b) ORDER BY a, b",
        "1,x,1;1,y,1;3,x,1;3,NULL,1;NULL,y,1",
    ),
    (
        "SELECT a, b, count(*) OVER () FROM t GROUP BY DISTINCT GROUPING SETS \
         ((a,b),(b,a),(t.a,b)) ORDER BY a,b",
        "1,x,5;1,y,5;3,x,5;3,NULL,5;NULL,y,5",
    ),
    (
        "SELECT coalesce(a, 0), grouping(coalesce(a, 0)), first_value(coalesce(a, 0)) OVER \
         (ORDER BY coalesce(a, 0) NULLS FIRST) FROM t GROUP BY ROLLUP(coalesce(a, 0)) ORDER BY \
         coalesce(a, 0)",
        "0,0,NULL;1,0,NULL;3,0,NULL;NULL,1,NULL",
    ),
    (
        "SELECT a, b, count(*) OVER () FROM t GROUP BY ALL (a,b) ORDER BY a, b",
        "1,x,5;1,y,5;3,x,5;3,NULL,5;NULL,y,5",
    ),
    (
        "SELECT a, b, count(*) OVER () FROM t GROUP BY ((a,b)) ORDER BY a, b",
        "1,x,5;1,y,5;3,x,5;3,NULL,5;NULL,y,5",
    ),
    (
        "SELECT a, count(*) OVER () FROM t WHERE a IN (SELECT 1) GROUP BY ROLLUP(a) \
         ORDER BY grouping(a), a",
        "1,2;NULL,2",
    ),
    (
        "SELECT a, lag(a, 0, 1 / (a - 1)) OVER (ORDER BY grouping(a), a) FROM t GROUP BY \
         ROLLUP(a) ORDER BY grouping(a), a",
        "1,1;3,3;NULL,NULL;NULL,NULL",
    ),
    (
        "SELECT 42, grouping(42::int), first_value(42::int) OVER (ORDER BY grouping(42::int) \
         DESC) FROM t GROUP BY ROLLUP(42::int) ORDER BY grouping(42::int)",
        "42,0,42;NULL,1,42",
    ),
    (
        "SELECT DISTINCT ON (grouping(a)) a, count(*) OVER () FROM t GROUP BY ROLLUP(a) \
         ORDER BY grouping(a), a",
        "1,4;NULL,4",
    ),
    (
        "SELECT DISTINCT a+1, count(*) OVER () FROM t GROUP BY ROLLUP(a) ORDER BY a+1",
        "2,4;4,4;NULL,4",
    ),
    (
        "SELECT a+1, lag(a+1, 0, 1/((a+1)-2)) OVER (ORDER BY grouping(a+1), a+1) FROM t \
         GROUP BY ROLLUP(a+1) ORDER BY grouping(a+1), a+1",
        "2,2;4,4;NULL,NULL;NULL,NULL",
    ),
    (
        "SELECT 0, count(*) OVER () FROM t GROUP BY GROUPING SETS (())",
        "0,1",
    ),
];

fn assert_rows(result: SqlResult, expected: &str, sql: &str) {
    let actual = result
        .rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .map(|value| match value {
                    Value::Null => "NULL".into(),
                    Value::Int(n) => n.to_string(),
                    Value::Numeric(n) => n.to_string(),
                    Value::Text(s) => s.clone(),
                    value => panic!("unexpected value {value:?} for {sql}"),
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>()
        .join(";");
    assert_eq!(actual, expected, "{sql}");
}

fn check_cases(session: &mut SqlSession) {
    for sql in SETUP {
        session.execute(sql, &[], None).unwrap();
    }
    for &(sql, expected) in CASES {
        let result = session
            .execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_rows(result, expected, sql);
    }

    for sql in [
        "SELECT a FROM t GROUP BY all all a",
        "SELECT a FROM t GROUP BY distinct all a",
        "SELECT a FROM t GROUP BY all (distinct a)",
    ] {
        let error = session.execute(sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "42601", "{sql}");
    }

    let sql = "SELECT a, b, grouping(a,b), count(*), count(*) OVER () FROM t GROUP BY CUBE(a,b) HAVING \
     count(*) > $1 ORDER BY grouping(a,b), a, b";
    let prepared = session.prepare(sql).unwrap();
    let result = session
        .execute_prepared(&prepared, &[Value::Int(1)], None)
        .unwrap();
    assert_rows(
        result,
        "1,NULL,1,2,5;3,NULL,1,2,5;NULL,x,2,2,5;NULL,y,2,2,5;NULL,NULL,3,5,5",
        sql,
    );
}

fn check_engine(engine: Engine) {
    let path = common::fresh("window-grouping-sets");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        check_cases(&mut admin.sql_session());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_matches_postgresql() {
    check_engine(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_matches_postgresql() {
    check_engine(Engine::V2);
}
