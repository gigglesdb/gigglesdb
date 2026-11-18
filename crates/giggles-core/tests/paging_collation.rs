mod common;
use giggles_core::{Admin, Engine, Limits, SqlSession, relational::Value as V};

fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn engines() -> Vec<Engine> {
    [
        Engine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        Engine::V2,
    ]
    .to_vec()
}
fn setup(engine: Engine) -> (Admin, SqlSession) {
    let admin =
        Admin::create_engine(common::fresh("paging-collation"), Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    run(
        &mut s,
        "CREATE COLLATION p2ci (provider=icu,locale='und-u-ks-level2',deterministic=false)",
    );
    run(
        &mut s,
        "CREATE TABLE p2c(id int PRIMARY KEY,t text COLLATE p2ci,n int)",
    );
    run(
        &mut s,
        "INSERT INTO p2c VALUES (1,'a',1),(2,'A',2),(3,'b',3),(4,'B',4),(5,'c',5)",
    );
    (admin, s)
}
fn pair(t: &str, n: i64) -> Vec<V> {
    vec![V::Text(t.into()), V::Int(n)]
}

// Expected results were verified on PostgreSQL 18.6 with ICU 76.1.
#[test]
fn distinct_on_uses_nondeterministic_key_equality() {
    for engine in engines() {
        let (_admin, mut s) = setup(engine);
        for sql in [
            "SELECT DISTINCT ON (t) t,id FROM p2c ORDER BY t,id",
            "SELECT DISTINCT ON (t COLLATE p2ci) t,id FROM p2c ORDER BY t COLLATE p2ci,id",
        ] {
            assert_eq!(
                run(&mut s, sql),
                vec![pair("a", 1), pair("b", 3), pair("c", 5)],
                "{sql}"
            );
        }
    }
}
#[test]
fn grouping_sets_rollup_cube_use_collated_groups() {
    for engine in engines() {
        let (_admin, mut s) = setup(engine);
        for syntax in ["GROUPING SETS ((t),())", "ROLLUP(t)", "CUBE(t)"] {
            let sql = format!("SELECT t,count(*) FROM p2c GROUP BY {syntax} ORDER BY t NULLS LAST");
            assert_eq!(
                run(&mut s, &sql),
                vec![
                    pair("a", 2),
                    pair("b", 2),
                    pair("c", 1),
                    vec![V::Null, V::Int(5)]
                ],
                "{sql}"
            );
        }
    }
}
#[test]
fn fetch_ties_use_order_expression_collations() {
    for engine in engines() {
        let (_admin, mut s) = setup(engine);
        for order in ["t", "t COLLATE p2ci", "t DESC"] {
            let (offset, expected) = if order.ends_with("DESC") {
                ("OFFSET 1 ROW", vec![pair("b", 3), pair("B", 4)])
            } else {
                ("", vec![pair("a", 1), pair("A", 2)])
            };
            let sql = format!(
                "SELECT t,id FROM p2c ORDER BY {order} {offset} FETCH FIRST 1 ROW WITH TIES"
            );
            assert_eq!(run(&mut s, &sql), expected, "{sql}");
        }
        assert_eq!(
            run(
                &mut s,
                "SELECT t,id FROM p2c ORDER BY t,id FETCH FIRST 1 ROW WITH TIES"
            ),
            vec![pair("a", 1)]
        );
        assert_eq!(
            run(
                &mut s,
                "SELECT DISTINCT ON (t) t,id FROM p2c ORDER BY t FETCH FIRST 1 ROW WITH TIES"
            ),
            vec![pair("a", 1)]
        );
        run(&mut s, "INSERT INTO p2c VALUES (6,NULL,6),(7,NULL,7)");
        assert_eq!(
            run(
                &mut s,
                "SELECT t,id FROM p2c ORDER BY t NULLS FIRST FETCH FIRST 1 ROW WITH TIES"
            ),
            vec![vec![V::Null, V::Int(6)], vec![V::Null, V::Int(7)]]
        );
    }
}
#[test]
fn deterministic_icu_order_under_offset_fetch() {
    for engine in engines() {
        let (_admin, mut s) = setup(engine);
        run(
            &mut s,
            "CREATE COLLATION p2de (provider=icu,locale='de-DE')",
        );
        run(&mut s, "CREATE TABLE german(t text COLLATE p2de)");
        run(
            &mut s,
            "INSERT INTO german VALUES ('z'),(chr(228)),('a'),('b')",
        );
        // PG18.6: a, U+00E4, b, z; bytes put U+00E4 after z.
        assert_eq!(
            run(
                &mut s,
                "SELECT t FROM german ORDER BY t OFFSET 1 ROW FETCH FIRST 2 ROWS ONLY"
            ),
            vec![vec![V::Text("\u{e4}".into())], vec![V::Text("b".into())]]
        );
        run(&mut s, "INSERT INTO german VALUES ('a')");
        assert_eq!(
            run(
                &mut s,
                "SELECT t FROM german ORDER BY t FETCH FIRST 1 ROW WITH TIES"
            ),
            vec![vec![V::Text("a".into())], vec![V::Text("a".into())]]
        );
    }
}
