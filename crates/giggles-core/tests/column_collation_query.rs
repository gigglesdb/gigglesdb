mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError as E, Value as V},
};
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
fn implicit_order_equality_groups_distinct_joins_in_and_like() {
    for engine in engines() {
        let a = Admin::create_engine(
            common::fresh("column-collation-query"),
            Limits::default(),
            engine,
        )
        .unwrap();
        let mut s = a.sql_session();
        setup(&mut s);
        assert_eq!(
            run(&mut s, "SELECT id FROM c1 ORDER BY a,id"),
            vec![vec![V::Int(3)], vec![V::Int(1)], vec![V::Int(2)]]
        );
        assert_eq!(
            run(&mut s, "SELECT id FROM c1 ORDER BY b,id"),
            vec![vec![V::Int(2)], vec![V::Int(3)], vec![V::Int(1)]]
        );
        for sql in [
            "SELECT id FROM c1 WHERE c='abc' ORDER BY id",
            "SELECT id FROM c1 WHERE c IN ('abc') ORDER BY id",
            "SELECT id FROM c1 WHERE c LIKE 'a%' ORDER BY id",
            "SELECT id FROM c1 WHERE c IN (SELECT c FROM c1 WHERE id=2) ORDER BY id",
        ] {
            assert_eq!(
                run(&mut s, sql),
                vec![vec![V::Int(1)], vec![V::Int(2)], vec![V::Int(3)]],
                "{sql}"
            );
        }
        assert_eq!(
            run(&mut s, "SELECT count(DISTINCT c) FROM c1"),
            vec![vec![V::Int(1)]]
        );
        assert_eq!(run(&mut s, "SELECT DISTINCT c FROM c1").len(), 1);
        assert_eq!(
            run(&mut s, "SELECT c,count(*) FROM c1 GROUP BY c")[0][1],
            V::Int(3)
        );
        assert_eq!(
            run(&mut s, "SELECT count(*) FROM c1 x JOIN c1 y ON x.c=y.c"),
            vec![vec![V::Int(9)]]
        );
    }
}
#[test]
fn derivation_conflicts_explicit_override_and_no_comparison() {
    for engine in engines() {
        let a = Admin::create_engine(
            common::fresh("column-collation-derive"),
            Limits::default(),
            engine,
        )
        .unwrap();
        let mut s = a.sql_session();
        setup(&mut s);
        assert_eq!(
            run(&mut s, "SELECT a<e,a=e FROM c1 WHERE id=1"),
            vec![vec![V::Bool(true), V::Bool(false)]]
        );
        for sql in [
            "SELECT a<b FROM c1",
            "SELECT a=b FROM c1",
            "SELECT (a||b)<'x' FROM c1",
        ] {
            assert_eq!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(E::IndeterminateCollation)),
                "{sql}"
            );
            assert_eq!(
                giggles_core::relational::take_sql_error_message().as_deref(),
                Some("could not determine which collation to use for string comparison")
            );
        }
        for sql in [
            "SELECT a||b FROM c1 ORDER BY 1",
            "SELECT a FROM c1 ORDER BY a||b",
            "SELECT a FROM c1 UNION SELECT b FROM c1",
        ] {
            assert_eq!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(E::CollationMismatch)),
                "{sql}"
            );
        }
        assert_eq!(
            run(&mut s, r#"SELECT a<b COLLATE "C" FROM c1 WHERE id=1"#),
            vec![vec![V::Bool(false)]]
        );
        assert_eq!(
            run(&mut s, "SELECT max(a||b) FROM c1 WHERE id=1"),
            vec![vec![V::Text("bb".into())]]
        );
        for sql in [
            "SELECT max(a||b) FROM c1",
            "SELECT min(a||b) FROM c1",
            "SELECT max(a||b) FROM c1 GROUP BY c",
            "SELECT greatest(a,b) FROM c1",
            "SELECT least(a,b) FROM c1",
            "SELECT (a||b) LIKE 'b%' FROM c1",
        ] {
            assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(E::IndeterminateCollation)), "{sql}");
        }
        // Aggregate sorting assigns collations at planning, even for a singleton.
        for sql in [
            "SELECT count(DISTINCT a||b) FROM c1",
            "SELECT max(a ORDER BY a||b) FROM c1",
            "SELECT count(a ORDER BY a||b) FROM c1",
            "SELECT max(a ORDER BY a||b) FROM c1 GROUP BY c",
            "SELECT max(a||b ORDER BY a||b) FROM c1 WHERE id=1",
            "SELECT count(DISTINCT a||b) FROM c1 WHERE id=1",
            "SELECT count(a ORDER BY a||b) FROM c1 WHERE id=1",
            "SELECT max(a ORDER BY a||b) FROM c1 WHERE id=1 GROUP BY c",
            "SELECT count(DISTINCT CASE WHEN id=1 THEN NULL ELSE a||b END ORDER BY CASE WHEN id=1 THEN NULL ELSE a||b END) FROM c1 WHERE id<=2",
        ] {
            assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(E::CollationMismatch)), "{sql}");
        }
        for sql in [
            "SELECT min(a||b) FROM c1 WHERE id=1",
            "SELECT greatest(a||b) FROM c1 WHERE id=1",
            "SELECT least(NULL,a||b) FROM c1 WHERE id=1",
        ] {
            assert_eq!(run(&mut s, sql), vec![vec![V::Text("bb".into())]], "{sql}");
        }
        assert_eq!(run(&mut s, "SELECT max(a ORDER BY a DESC NULLS LAST) FROM c1"),vec![vec![V::Text("B".into())]]);
        assert_eq!(run(&mut s, "SELECT count(DISTINCT c ORDER BY c) FROM c1"),vec![vec![V::Int(1)]]);
        assert_eq!(s.execute("SELECT count(DISTINCT c ORDER BY id) FROM c1",&[],None),Err(CoreError::Sql(E::InvalidColumnReference)));
        assert_eq!(
            run(
                &mut s,
                "SELECT CASE WHEN true THEN a ELSE b END FROM c1 WHERE id=1"
            ),
            vec![vec![V::Text("b".into())]]
        );
        // Preserve enum ordering through the same scalar and aggregate paths.
        run(&mut s, "CREATE TYPE enum_order AS ENUM('z','a')");
        run(&mut s, "CREATE TABLE enum_values(id int PRIMARY KEY,v enum_order)");
        run(&mut s, "INSERT INTO enum_values VALUES(1,'z'),(2,'a')");
        assert_eq!(run(&mut s, "SELECT greatest(v,'z'),least(v,'a') FROM enum_values ORDER BY id"),
            vec![vec![V::Text("z".into()),V::Text("z".into())],vec![V::Text("a".into()),V::Text("a".into())]]);
        assert_eq!(run(&mut s, "SELECT min(v ORDER BY v),max(v ORDER BY v DESC) FROM enum_values"),
            vec![vec![V::Text("z".into()),V::Text("a".into())]]);
    }
}

#[test]
fn in_conflicts_wait_for_non_null_comparison() {
    for engine in engines() {
        let admin = Admin::create_engine(
            common::fresh("column-collation-in"),
            Limits::default(),
            engine,
        ).unwrap();
        let mut session = admin.sql_session();
        setup(&mut session);
        for sql in [
            "SELECT id FROM c1 WHERE a||b IN (SELECT b FROM c1 WHERE id<0)",
            "SELECT id FROM c1 WHERE a||b IN (SELECT CASE WHEN id>0 THEN NULL ELSE b END FROM c1)",
            "SELECT id FROM c1 WHERE (CASE WHEN id>0 THEN NULL ELSE a||b END) IN (SELECT b FROM c1)",
        ] {
            assert!(run(&mut session, sql).is_empty(), "{sql}");
        }
        for sql in [
            "SELECT id FROM c1 WHERE a||b IN (SELECT b FROM c1)",
            "SELECT id FROM c1 WHERE a||b IN (SELECT CASE WHEN id=1 THEN NULL ELSE b END FROM c1)",
        ] {
            assert_eq!(session.execute(sql, &[], None), Err(CoreError::Sql(E::IndeterminateCollation)), "{sql}");
            assert_eq!(giggles_core::relational::take_sql_error_message().as_deref(),
                Some("could not determine which collation to use for string comparison"));
        }
    }
}

#[test]
fn bpchar_consumers_fail_closed_with_column_and_expression_collations() {
    for engine in engines() {
        let admin = Admin::create_engine(
            common::fresh("column-collation-bpchar"),
            Limits::default(),
            engine,
        )
        .unwrap();
        let mut session = admin.sql_session();
        run(
            &mut session,
            "CREATE COLLATION ci(provider=icu,locale='und-u-ks-level2',deterministic=false)",
        );
        for collation in ["", " COLLATE \"C\"", " COLLATE \"und-x-icu\"", " COLLATE ci"] {
            run(
                &mut session,
                &format!("CREATE TABLE bp(id int PRIMARY KEY,c char(3){collation},v varchar(3){collation})"),
            );
            // Binding rejects the consumers even before there are input rows.
            for populated in [false, true] {
                if populated {
                    run(&mut session, "INSERT INTO bp VALUES(1,'a','a'),(2,'b','b')");
                }
                for input in ["c", "c COLLATE \"C\"", "c COLLATE \"und-x-icu\"", "c COLLATE ci"] {
                    for sql in [
                        format!("SELECT {input}='a' FROM bp"),
                        format!("SELECT 'a'={input} FROM bp"),
                        format!("SELECT {input}<v FROM bp"),
                        format!("SELECT v>{input} FROM bp"),
                        format!("SELECT id FROM bp WHERE {input}='a'"),
                        format!("SELECT id FROM bp WHERE {input} IN ('a','b')"),
                        format!("SELECT id FROM bp WHERE {input} IN (SELECT c FROM bp)"),
                        format!("SELECT c FROM bp ORDER BY {input}"),
                        format!("SELECT min({input}),max({input}) FROM bp"),
                        format!("SELECT min({input}),max({input}) FROM bp GROUP BY id"),
                        format!("SELECT count(DISTINCT {input}) FROM bp"),
                        format!("SELECT count(v ORDER BY {input}) FROM bp"),
                        format!("SELECT min(v ORDER BY {input}) FROM bp GROUP BY id"),
                        format!("SELECT {input},count(*) FROM bp GROUP BY {input}"),
                        format!("SELECT greatest({input},'a'),least({input},'b') FROM bp"),
                    ] {
                        assert_eq!(
                            session.execute(&sql, &[], None),
                            Err(CoreError::Sql(E::Unsupported)),
                            "{engine:?}, column{collation}, populated={populated}: {sql}"
                        );
                    }
                }
                for sql in [
                    "SELECT x.id FROM bp x JOIN bp y ON x.c=y.c",
                    "SELECT c FROM bp ORDER BY 1",
                ] {
                    assert_eq!(session.execute(sql, &[], None), Err(CoreError::Sql(E::Unsupported)), "{sql}");
                }
            }
            run(&mut session, "DROP TABLE bp");
        }
        for sql in [
            "SELECT ('a'::char(3) COLLATE \"C\")='a'",
            "SELECT 'a'=('a'::char(3) COLLATE \"und-x-icu\")",
            "SELECT 'a'::char(3) ORDER BY 1",
            "SELECT min('a'::char(3) COLLATE ci)",
        ] {
            assert_eq!(session.execute(sql, &[], None), Err(CoreError::Sql(E::Unsupported)), "{sql}");
        }
    }
}
