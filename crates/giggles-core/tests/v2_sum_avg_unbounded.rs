#![cfg(feature = "engine-rocksdb")]
//! R2-PGLIMITS-1: on v2, SUM/AVG keep constant-size running state and never hit
//! a statement-wide work cap (PostgreSQL streams aggregates; no 54000).
mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits,
    relational::{SqlError, Value},
};

const ROWS: i64 = 300_000;

fn text(v: &Value) -> String {
    match v {
        Value::Numeric(n) => n.to_string(),
        Value::Int(n) => n.to_string(),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn sum_and_avg_over_300_000_rows_match_postgresql() {
    let path = common::fresh("v2-sum-avg-unbounded");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        s.execute(
            "CREATE TABLE t (a int PRIMARY KEY, b bigint, c numeric)",
            &[],
            None,
        )
        .unwrap();
        for start in (0..ROWS).step_by(10_000) {
            let values = (start..start + 10_000)
                .map(|i| format!("({i},{},{i}.25)", i * 1_000_000))
                .collect::<Vec<_>>()
                .join(",");
            s.execute(&format!("INSERT INTO t VALUES {values}"), &[], None)
                .unwrap();
        }
        // PG18: sum(int4)=int8, sum(int8)/sum(numeric)/avg(*)=numeric; avg scale
        // follows select_div_scale (16 significant digits, at least the input dscale).
        let r = s
            .execute(
                "SELECT sum(a), avg(a), sum(b), avg(b), sum(c), avg(c), count(a), max(c) FROM t",
                &[],
                None,
            )
            .unwrap();
        let got = r.rows[0].values.iter().map(text).collect::<Vec<_>>();
        assert_eq!(
            got,
            [
                "44999850000",
                "149999.500000000000",
                "44999850000000000",
                "149999500000.00000000",
                "44999925000.00",
                "149999.750000000000",
                "300000",
                "299999.25",
            ]
        );
        // Grouped path: each group keeps its own running sum and count.
        let r = s
            .execute("SELECT a % 3, sum(a), sum(c) FROM t GROUP BY a % 3 ORDER BY 1", &[], None)
            .unwrap();
        let got = r
            .rows
            .iter()
            .map(|row| row.values.iter().map(text).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let expect = (0..3)
            .map(|g| {
                let ids = (0..ROWS).filter(|i| i % 3 == g);
                let n = ids.clone().count() as i64;
                let sum: i64 = ids.sum();
                vec![
                    g.to_string(),
                    sum.to_string(),
                    format!("{}.{:02}", sum + n / 4, (n % 4) * 25),
                ]
            })
            .collect::<Vec<_>>();
        assert_eq!(got, expect);
        // Computed arguments still share DD-0068's statement-wide scalar budget.
        assert_eq!(
            s.execute("SELECT sum(length(repeat(c::text, 4096))) FROM t", &[], None),
            Err(CoreError::Sql(SqlError::Limit))
        );
    }
    let _ = std::fs::remove_dir_all(&path);
}
