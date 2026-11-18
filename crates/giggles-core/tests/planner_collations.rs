//! PostgreSQL 18.6 planner collation regression cases.
mod common;
use giggles_core::{Admin, Engine, Limits, SqlSession, relational::Value as V};
fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| {
            panic!(
                "{sql}: {e:?} {:?}",
                giggles_core::relational::take_sql_error_message()
            )
        })
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn run_affected(s: &mut SqlSession, sql: &str, affected: u64) -> Vec<Vec<V>> {
    let result = s
        .execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(result.affected, affected, "{sql}");
    result.rows.into_iter().map(|r| r.values).collect()
}
fn fixture() -> (Admin, SqlSession) {
    let a = Admin::create_engine(
        common::fresh("planner-collations"),
        Limits::default(),
        #[cfg(feature = "engine-rocksdb")]
        Engine::V2,
        #[cfg(not(feature = "engine-rocksdb"))]
        Engine::Legacy,
    )
    .unwrap();
    let mut s = a.sql_session();
    for sql in [
        "CREATE COLLATION p2ci(provider=icu,locale='und-u-ks-level2',deterministic=false)",
        "CREATE TABLE ca(id int PRIMARY KEY,t text COLLATE p2ci)",
        "CREATE TABLE cb(id int PRIMARY KEY,t text COLLATE p2ci)",
        "CREATE TABLE cc(id int PRIMARY KEY,t text COLLATE \"C\")",
        "INSERT INTO ca VALUES(1,'a'),(2,'B'),(3,'c')",
        "INSERT INTO cb VALUES(10,'A'),(20,'b'),(30,'d')",
        "INSERT INTO cc VALUES(100,'A'),(200,'b')",
    ] {
        run(&mut s, sql);
    }
    (a, s)
}
fn ids() -> Vec<Vec<V>> {
    vec![vec![V::Int(1), V::Int(10)], vec![V::Int(2), V::Int(20)]]
}
#[test]
fn joins_subqueries_lateral_and_sets() {
    let (_a, mut s) = fixture();
    for sql in [
        "SELECT ca.id,cb.id FROM ca JOIN cb ON ca.t=cb.t ORDER BY 1",
        "SELECT ca.id,x.id FROM ca CROSS JOIN LATERAL(SELECT id FROM cb WHERE cb.t=ca.t)x ORDER BY 1",
    ] {
        assert_eq!(run(&mut s, sql), ids(), "{sql}");
    }
    assert_eq!(
        run(
            &mut s,
            "SELECT ca.id,cb.id FROM ca FULL JOIN cb USING(t) ORDER BY 1,2"
        ),
        vec![
            vec![V::Int(1), V::Int(10)],
            vec![V::Int(2), V::Int(20)],
            vec![V::Int(3), V::Null],
            vec![V::Null, V::Int(30)]
        ]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT t,ca.id,cb.id FROM ca NATURAL LEFT JOIN cb ORDER BY 2"
        ),
        vec![
            vec![V::Text("a".into()), V::Int(1), V::Null],
            vec![V::Text("B".into()), V::Int(2), V::Null],
            vec![V::Text("c".into()), V::Int(3), V::Null]
        ]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM ca WHERE t IN(SELECT t FROM cb) ORDER BY id"
        ),
        vec![vec![V::Int(1)], vec![V::Int(2)]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id,t=ANY(SELECT t FROM cb) FROM ca ORDER BY id"
        ),
        vec![
            vec![V::Int(1), V::Bool(true)],
            vec![V::Int(2), V::Bool(true)],
            vec![V::Int(3), V::Bool(false)]
        ]
    );
    assert_eq!(
        run(&mut s, "SELECT t FROM ca UNION SELECT t FROM cb ORDER BY 1"),
        ["a", "b", "c", "d"]
            .map(|v| vec![V::Text(v.into())])
            .to_vec()
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT x FROM unnest(ARRAY['a','A','b']::text[] COLLATE p2ci)x WHERE x='a' ORDER BY 1"
        ),
        vec![vec![V::Text("a".into())], vec![V::Text("A".into())]]
    );
}
#[test]
fn conflicting_collations() {
    let (_a, mut s) = fixture();
    for sql in [
        "SELECT ca.id FROM ca JOIN cc ON ca.t=cc.t",
        "SELECT ca.id FROM ca WHERE t IN(SELECT t FROM cc)",
    ] {
        let e = s.execute(sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&e), "42P22", "{sql}: {e:?}");
    }
    assert!(
        run(
            &mut s,
            "SELECT ca.id FROM ca JOIN cc ON ca.t=cc.t COLLATE \"C\" ORDER BY 1"
        )
        .is_empty()
    );
    let e = s
        .execute(
            "SELECT ca.id FROM ca JOIN cc ON ca.t COLLATE p2ci=cc.t COLLATE \"C\"",
            &[],
            None,
        )
        .unwrap_err();
    assert_eq!(giggles_core::sqlstate(&e), "42P21");
}
#[test]
fn joined_mutations() {
    let (_a, mut s) = fixture();
    run(&mut s, "BEGIN");
    assert_eq!(
        run_affected(
            &mut s,
            "UPDATE ca SET id=ca.id+1000 FROM cb WHERE ca.t=cb.t RETURNING ca.id,cb.id",
            2
        ),
        vec![
            vec![V::Int(1001), V::Int(10)],
            vec![V::Int(1002), V::Int(20)]
        ]
    );
    run(&mut s, "ROLLBACK");
    run(&mut s, "BEGIN");
    assert_eq!(
        run_affected(
            &mut s,
            "DELETE FROM ca USING cb WHERE ca.t=cb.t RETURNING ca.id",
            2
        ),
        vec![vec![V::Int(1)], vec![V::Int(2)]]
    );
    run(&mut s, "ROLLBACK");
    assert_eq!(
        run_affected(
            &mut s,
            "MERGE INTO ca t USING cb s ON t.t=s.t WHEN MATCHED THEN UPDATE SET id=t.id+1000 WHEN NOT MATCHED THEN INSERT VALUES(s.id,s.t) RETURNING merge_action(),t.id,t.t",
            3
        ),
        vec![
            vec![V::Text("UPDATE".into()), V::Int(1001), V::Text("a".into())],
            vec![V::Text("UPDATE".into()), V::Int(1002), V::Text("B".into())],
            vec![V::Text("INSERT".into()), V::Int(30), V::Text("d".into())]
        ]
    );
}
#[test]
fn deterministic_icu_equality_uses_byte_tiebreak() {
    let (_a, mut s) = fixture();
    run(&mut s, "CREATE COLLATION det(provider=icu,locale='de-DE')");
    run(
        &mut s,
        "CREATE TABLE da(id int PRIMARY KEY,t text COLLATE det)",
    );
    run(
        &mut s,
        "CREATE TABLE db(id int PRIMARY KEY,t text COLLATE det)",
    );
    run(&mut s, "INSERT INTO da VALUES(1,'\u{e4}'),(2,'z')");
    run(&mut s, "INSERT INTO db VALUES(10,'a\u{308}'),(20,'z')");
    assert_eq!(
        run(
            &mut s,
            "SELECT da.id,db.id FROM da JOIN db ON da.t=db.t ORDER BY 1"
        ),
        vec![vec![V::Int(2), V::Int(20)]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM da WHERE t IN(SELECT t FROM db) ORDER BY 1"
        ),
        vec![vec![V::Int(2)]]
    );
}

#[test]
fn ctes_applies_and_multiset_equality() {
    let (_a, mut s) = fixture();
    for sql in [
        "WITH a AS(SELECT * FROM ca),b AS(SELECT * FROM cb) SELECT a.id,b.id FROM a JOIN b ON a.t=b.t ORDER BY 1",
        "SELECT ca.id,cb.id FROM ca RIGHT JOIN cb ON ca.t=cb.t WHERE ca.id IS NOT NULL ORDER BY 1",
    ] {
        assert_eq!(run(&mut s, sql), ids(), "{sql}");
    }
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM ca WHERE t NOT IN(SELECT t FROM cb) ORDER BY 1"
        ),
        vec![vec![V::Int(3)]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM ca WHERE t=ALL(SELECT t FROM cb WHERE id=10) ORDER BY 1"
        ),
        vec![vec![V::Int(1)]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT t FROM ca INTERSECT SELECT t FROM cb ORDER BY 1"
        ),
        vec![vec![V::Text("a".into())], vec![V::Text("B".into())]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT t FROM ca EXCEPT SELECT t FROM cb ORDER BY 1"
        ),
        vec![vec![V::Text("c".into())]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM ca WHERE (SELECT t FROM cb WHERE id=10)=ca.t ORDER BY 1"
        ),
        vec![vec![V::Int(1)]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT x FROM (SELECT 'a' COLLATE p2ci x) q WHERE x='A'"
        ),
        vec![vec![V::Text("a".into())]]
    );
    assert!(
        run(
            &mut s,
            "SELECT x FROM (SELECT 'a' COLLATE p2ci x) q WHERE x='A' COLLATE \"C\""
        )
        .is_empty()
    );
}

#[test]
fn aggregate_apply_outputs_preserve_collations() {
    let (_a, mut s) = fixture();
    assert_eq!(
        run(
            &mut s,
            "SELECT x FROM (SELECT min(t) x FROM ca WHERE EXISTS(SELECT 1 FROM cb WHERE cb.t=ca.t)) q WHERE x='A'"
        ),
        vec![vec![V::Text("a".into())]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT x FROM (SELECT min(t) x FROM ca WHERE EXISTS(SELECT 1 FROM cb WHERE cb.t=ca.t) GROUP BY t) q WHERE x='b'"
        ),
        vec![vec![V::Text("B".into())]]
    );
}

#[test]
fn recursive_collations_preserve_equality_conflicts_and_path_order() {
    let (_a, mut s) = fixture();
    assert_eq!(
        run(
            &mut s,
            "WITH RECURSIVE r(x) AS(SELECT 'a'::text COLLATE p2ci UNION SELECT 'A'::text COLLATE p2ci FROM r) SELECT x FROM r"
        ),
        vec![vec![V::Text("a".into())]]
    );
    for sql in [
        "WITH RECURSIVE r(x) AS(SELECT 'a'::text COLLATE p2ci UNION SELECT x COLLATE \"C\" FROM r WHERE false) SELECT * FROM r",
        "WITH RECURSIVE r(x) AS(SELECT 'a'::text UNION ALL SELECT x COLLATE p2ci FROM r WHERE false) SELECT * FROM r",
    ] {
        let e = s.execute(sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&e), "42P21", "{sql}: {e:?}");
    }
    for decoration in [
        "SEARCH DEPTH FIRST BY t SET ord",
        "SEARCH BREADTH FIRST BY t SET ord",
        "CYCLE t SET cyclic USING ord",
    ] {
        let sql = format!(
            "WITH RECURSIVE r(id,t) AS(SELECT id,t FROM ca UNION ALL SELECT id,t FROM r WHERE false) {decoration} SELECT id FROM r ORDER BY ord"
        );
        assert_eq!(
            run(&mut s, &sql),
            vec![vec![V::Int(1)], vec![V::Int(2)], vec![V::Int(3)]],
            "{sql}"
        );
    }
    assert_eq!(
        run(
            &mut s,
            "WITH RECURSIVE r(t) AS(SELECT 'a'::text COLLATE p2ci UNION ALL SELECT 'A'::text COLLATE p2ci FROM r) CYCLE t SET cyclic USING path SELECT t,cyclic FROM r"
        ),
        vec![
            vec![V::Text("a".into()), V::Bool(false)],
            vec![V::Text("A".into()), V::Bool(true)]
        ]
    );
}

#[test]
fn using_output_collations_follow_selected_values() {
    let (_a, mut s) = fixture();
    for sql in [
        "CREATE TABLE up(id int PRIMARY KEY,t text)",
        "CREATE TABLE uc(id int PRIMARY KEY,t text COLLATE p2ci)",
        "CREATE TABLE uv10(id int PRIMARY KEY,t varchar(10))",
        "CREATE TABLE uvc10(id int PRIMARY KEY,t varchar(10) COLLATE p2ci)",
        "CREATE TABLE uvc20(id int PRIMARY KEY,t varchar(20) COLLATE p2ci)",
        "CREATE TABLE uvc(id int PRIMARY KEY,t varchar COLLATE p2ci)",
        "INSERT INTO up VALUES(1,'a')",
        "INSERT INTO uc VALUES(1,'A')",
        "INSERT INTO uv10 VALUES(1,'a')",
        "INSERT INTO uvc10 VALUES(1,'A')",
        "INSERT INTO uvc20 VALUES(1,'A')",
        "INSERT INTO uvc VALUES(1,'A')",
    ] {
        run(&mut s, sql);
    }
    for natural in [false, true] {
        for (left, right) in [("up", "uc"), ("uc", "up")] {
            for kind in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] {
                let right_value = if kind == "RIGHT JOIN" {
                    right == "uc"
                } else {
                    left == "uc"
                };
                let ci = if kind == "FULL JOIN" {
                    true
                } else {
                    right_value
                };
                let sql = format!(
                    "SELECT t,t='A' FROM {left} {}{kind} {right}{}",
                    if natural { "NATURAL " } else { "" },
                    if natural { "" } else { " USING(t)" }
                );
                assert_eq!(
                    run(&mut s, &sql),
                    vec![vec![
                        V::Text(if right_value { "A" } else { "a" }.into()),
                        V::Bool(ci)
                    ]],
                    "{sql}"
                );
            }
        }
    }
    for (left, right, value, ci) in [
        ("uv10", "uc", "a", false),
        ("up", "uvc10", "a", false),
        ("uv10", "uvc", "A", true),
        ("uv10", "uvc20", "a", false),
    ] {
        let sql = format!("SELECT t,t='A' FROM {left} JOIN {right} USING(t)");
        assert_eq!(
            run(&mut s, &sql),
            vec![vec![V::Text(value.into()), V::Bool(ci)]],
            "{sql}"
        );
    }
}
