mod common;
use giggles_core::{Admin, Engine, Limits, SqlSession, relational::Value as V};
fn engines() -> Vec<Engine> {
    [Engine::Legacy, #[cfg(feature = "engine-rocksdb")] Engine::V2].to_vec()
}
fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| {
            panic!(
                "{sql}: {e:?} message {:?}",
                giggles_core::relational::take_sql_error_message()
            )
        })
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn setup(s: &mut SqlSession) {
    run(
        s,
        "CREATE COLLATION ci (provider=icu,locale='und-u-ks-level2',deterministic=false)",
    );
    run(
        s,
        r#"CREATE TABLE c1(id int PRIMARY KEY,a text COLLATE "und-x-icu",b text COLLATE "C",c text COLLATE ci,d varchar(10) COLLATE "de-x-icu",e text)"#,
    );
    run(
        s,
        "INSERT INTO c1 VALUES(1,'b','b','ABC','ä','x'),(2,'B','B','abc','a','y'),(3,'a','a','Abc','z','z')",
    );
}

#[test]
fn collated_indexes_preserve_queries_and_restart() {
    for engine in engines() {
        let path = common::fresh("column-collation-index");
        {
            let a = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            let mut s = a.sql_session();
            setup(&mut s);
            run(&mut s, "CREATE INDEX ON c1(a)");
            run(&mut s, r#"CREATE INDEX ON c1(e COLLATE "C")"#);
            run(&mut s, "CREATE INDEX ON c1(c)");
            assert_eq!(
                run(&mut s, "SELECT id FROM c1 WHERE a<'B' ORDER BY a,id"),
                vec![vec![V::Int(3)], vec![V::Int(1)]]
            );
            assert_eq!(
                run(&mut s, "SELECT count(*) FROM c1 WHERE c='abc'"),
                vec![vec![V::Int(3)]]
            );
            assert_eq!(
                run(&mut s, "SELECT count(*) FROM c1 WHERE a='b'"),
                vec![vec![V::Int(1)]]
            );
        }
        let a = Admin::open(&path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        assert_eq!(
            run(&mut s, "SELECT id FROM c1 WHERE a<'B' ORDER BY a,id"),
            vec![vec![V::Int(3)], vec![V::Int(1)]]
        );
        assert!(
            s.collation_oids()
                .unwrap()
                .values()
                .any(|v| v == &vec![950])
        );
        run(&mut s, "DROP INDEX c1_e_idx");
        run(&mut s, "DROP TABLE c1");
        run(&mut s, "DROP COLLATION ci");
    }
}
