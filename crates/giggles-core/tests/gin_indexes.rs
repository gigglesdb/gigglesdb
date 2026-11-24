mod common;

use giggles_core::{Admin, Engine, Limits, SqlSession, relational::Value, sqlstate};

fn run(sql: &mut SqlSession, statement: &str) -> Vec<Vec<Value>> {
    sql.execute(statement, &[], None)
        .unwrap_or_else(|error| panic!("{statement}: {error:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn lifecycle(engine: Engine) {
    let path = common::fresh("gin-indexes");
    let limits = Limits::default();
    let admin = Admin::create_engine(&path, limits, engine).unwrap();
    let mut sql = admin.sql_session();
    run(
        &mut sql,
        "CREATE TABLE items (id int PRIMARY KEY, v int[], t text, j jsonb, more text[])",
    );
    run(
        &mut sql,
        "INSERT INTO items VALUES (1,'{1,2}','a','{\"k\":1}','{a}'), (2,'{2,3}','b','{\"k\":2}','{b}')",
    );
    let before = run(&mut sql, "SELECT id,v,j FROM items ORDER BY id");
    run(&mut sql, "CREATE INDEX items_v ON items USING GIN (v)");
    run(
        &mut sql,
        "CREATE INDEX items_j ON items USING GIN (j jsonb_path_ops) WITH (fastupdate=off,gin_pending_list_limit=128)",
    );
    run(
        &mut sql,
        "CREATE INDEX items_multi ON items USING gin (v,more)",
    );
    run(
        &mut sql,
        "CREATE INDEX IF NOT EXISTS items_multi ON items USING gin (v,more)",
    );
    assert_eq!(
        run(&mut sql, "SELECT id,v,j FROM items ORDER BY id"),
        before
    );
    for (statement, expected) in [
        ("CREATE INDEX items_t ON items USING gin (t)", "42704"),
        (
            "CREATE UNIQUE INDEX items_u ON items USING gin (v)",
            "0A000",
        ),
        ("CREATE INDEX items_none ON items USING nosuch (v)", "42704"),
        (
            "CREATE INDEX items_expr ON items USING gin (to_tsvector('english', t))",
            "0A000",
        ),
        ("CREATE INDEX items_v ON items USING gin (v)", "42P07"),
    ] {
        assert_eq!(
            sqlstate(&sql.execute(statement, &[], None).unwrap_err()),
            expected,
            "{statement}"
        );
    }
    drop(sql);
    drop(admin);
    let admin = Admin::open(&path, limits).unwrap();
    let mut sql = admin.sql_session();
    let indexes = sql.catalog_metadata().unwrap().presentation.gin_indexes;
    assert_eq!(indexes.len(), 3);
    let json = indexes
        .values()
        .find(|index| index.name == "items_j")
        .unwrap();
    assert_eq!(json.opclasses, [10091]);
    assert_eq!(
        json.options,
        [
            ("fastupdate".into(), "off".into()),
            ("gin_pending_list_limit".into(), "128".into())
        ]
    );
    run(&mut sql, "ALTER INDEX items_v RENAME TO items_array");
    run(&mut sql, "DROP INDEX items_array");
    run(&mut sql, "DROP INDEX IF EXISTS items_array");
    run(&mut sql, "ALTER TABLE items DROP COLUMN more");
    assert_eq!(
        sql.catalog_metadata()
            .unwrap()
            .presentation
            .gin_indexes
            .len(),
        1
    );
    run(&mut sql, "DROP TABLE items");
    assert!(
        sql.catalog_metadata()
            .unwrap()
            .presentation
            .gin_indexes
            .is_empty()
    );
    drop(sql);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn gin_indexes_legacy_lifecycle() {
    lifecycle(Engine::Legacy);
}
#[test]
#[cfg(feature = "engine-rocksdb")]
fn gin_indexes_v2_lifecycle() {
    lifecycle(Engine::V2);
}
