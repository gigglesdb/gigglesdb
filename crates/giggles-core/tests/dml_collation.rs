#![cfg(feature = "engine-rocksdb")]
//! Joined mutation metadata and equality follow PostgreSQL 18.6.
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
#[cfg(feature = "engine-rocksdb")]
fn fixture(collation: &str) -> (Admin, SqlSession) {
    let admin = Admin::create_engine(
        common::fresh("dml-collation"),
        Limits::default(),
        Engine::V2,
    )
    .unwrap();
    let mut s = admin.sql_session();
    run(
        &mut s,
        "CREATE COLLATION ci (provider=icu,locale='und-u-ks-level2',deterministic=false)",
    );
    run(
        &mut s,
        &format!("CREATE TABLE t(id int PRIMARY KEY,k text COLLATE {collation},v int)"),
    );
    run(
        &mut s,
        &format!("CREATE TABLE src(id int PRIMARY KEY,k text COLLATE {collation},v int)"),
    );
    run(&mut s, "INSERT INTO t VALUES(1,'abc',10)");
    run(&mut s, "INSERT INTO src VALUES(2,'ABC',20),(3,'zzz',30)");
    (admin, s)
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn joined_dml_preserves_implicit_collations() {
    for collation in ["ci", "\"und-x-icu\""] {
        let (_admin, mut s) = fixture(collation);
        for sql in [
            "UPDATE t SET v=src.v FROM src WHERE t.k=src.k RETURNING old.k,new.k",
            "DELETE FROM t USING src WHERE t.k=src.k RETURNING old.k,new.k",
            "MERGE INTO t USING src ON t.k=src.k WHEN MATCHED THEN UPDATE SET v=src.v WHEN NOT MATCHED THEN INSERT VALUES(src.id,src.k,src.v) RETURNING old.k,new.k",
            "MERGE INTO t USING src ON t.id=src.id WHEN NOT MATCHED BY SOURCE AND t.k='ABC' THEN DELETE",
            "WITH changed AS(UPDATE t SET v=src.v FROM src WHERE t.k=src.k RETURNING t.id) SELECT * FROM changed",
            "WITH source AS(SELECT * FROM src) UPDATE t SET v=source.v FROM source WHERE t.k=source.k",
            "WITH source AS(SELECT * FROM src) MERGE INTO t USING source ON t.k=source.k WHEN MATCHED THEN DELETE",
        ] {
            run(&mut s, "BEGIN");
            run(&mut s, sql);
            run(&mut s, "ROLLBACK");
            assert_eq!(
                run(&mut s, "SELECT id,k,v FROM t"),
                vec![vec![V::Int(1), V::Text("abc".into()), V::Int(10)]]
            );
        }
    }
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn ordinary_returning_images_preserve_column_collations() {
    let (_admin, mut s) = fixture("ci");
    assert_eq!(
        run(
            &mut s,
            "UPDATE t SET k='def' RETURNING old.k,new.k,old.k='ABC',new.k='DEF'"
        ),
        vec![vec![
            V::Text("abc".into()),
            V::Text("def".into()),
            V::Bool(true),
            V::Bool(true)
        ]]
    );
    assert_eq!(
        run(&mut s, "DELETE FROM t RETURNING old.k,new.k,old.k='DEF'"),
        vec![vec![V::Text("def".into()), V::Null, V::Bool(true)]]
    );
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn joined_dml_preserves_explicit_collations_in_all_expression_positions() {
    let admin = Admin::create_engine(
        common::fresh("dml-explicit-collation"),
        Limits::default(),
        Engine::V2,
    )
    .unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TABLE t(id int PRIMARY KEY,k text)");
    run(&mut s, "INSERT INTO t VALUES(1,'abc')");
    for sql in [
        "UPDATE t SET k='x' FROM t src WHERE t.k COLLATE \"und-x-icu\"=src.k",
        "DELETE FROM t USING t src WHERE t.k=src.k COLLATE \"und-x-icu\"",
        "UPDATE t SET k=src.k COLLATE \"und-x-icu\" FROM t src WHERE t.id=src.id",
        "UPDATE t SET k=src.k FROM t src WHERE t.id=src.id RETURNING old.k COLLATE \"und-x-icu\"",
        "MERGE INTO t USING t src ON t.k COLLATE \"und-x-icu\"=src.k WHEN MATCHED THEN DELETE",
        "MERGE INTO t USING t src ON t.id=src.id WHEN MATCHED AND t.k COLLATE \"und-x-icu\"=src.k THEN DELETE",
        "MERGE INTO t USING t src ON false WHEN NOT MATCHED THEN INSERT VALUES(2,src.k COLLATE \"und-x-icu\")",
        "MERGE INTO t USING t src ON true WHEN MATCHED THEN DO NOTHING RETURNING old.k COLLATE \"und-x-icu\"",
    ] {
        run(&mut s, "BEGIN");
        run(&mut s, sql);
        run(&mut s, "ROLLBACK");
    }
    assert_eq!(
        run(&mut s, "SELECT k FROM t"),
        vec![vec![V::Text("abc".into())]]
    );
}

#[test]
fn cte_source_collations_survive_materialization() {
    let (_admin, mut s) = fixture("ci");
    run(
        &mut s,
        "CREATE TABLE plain(id int PRIMARY KEY,k text,v int)",
    );
    run(&mut s, "INSERT INTO plain VALUES(1,'abc',10)");
    for sql in [
        "WITH source AS(SELECT * FROM src) UPDATE plain SET v=source.v FROM source WHERE plain.k=source.k",
        "WITH source AS(SELECT * FROM src) DELETE FROM plain USING source WHERE plain.k=source.k",
        "WITH source AS(SELECT * FROM src) MERGE INTO plain USING source ON plain.k=source.k WHEN MATCHED THEN DELETE",
    ] {
        run(&mut s, "BEGIN");
        run(&mut s, sql);
        run(&mut s, "ROLLBACK");
    }
    assert_eq!(run(&mut s, "SELECT id FROM plain"), vec![vec![V::Int(1)]]);
}
