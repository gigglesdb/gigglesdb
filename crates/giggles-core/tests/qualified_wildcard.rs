mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{DataType, SqlError, Value},
};

fn query(s: &mut SqlSession, sql: &str, names: &[&str]) -> Vec<Vec<Value>> {
    let prepared = s.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    let description = s
        .describe(&prepared)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(
        description
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        names,
        "{sql}"
    );
    assert!(
        description
            .columns
            .iter()
            .all(|c| c.name != "giggles_rowid")
    );
    let result = s
        .execute_prepared(&prepared, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(
        result.columns,
        description
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>(),
        "{sql}"
    );
    let mut cursor = s.open_cursor(&prepared, &[]).unwrap();
    let mut rows = Vec::new();
    while !cursor.is_exhausted() {
        rows.extend(cursor.fetch(1, 4096).unwrap());
    }
    assert_eq!(rows, result.rows, "{sql}");
    result.rows.into_iter().map(|r| r.values).collect()
}

fn exercise(engine: Engine) {
    let path = common::fresh("qualified-wildcard");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for sql in [
        "CREATE TABLE table_name (test integer)",
        "INSERT INTO table_name VALUES (2),(1),(3)",
        "CREATE TABLE keyed (id integer PRIMARY KEY, label text)",
        "INSERT INTO keyed VALUES (1,'one'),(2,'two')",
        "CREATE SCHEMA other",
        "CREATE TABLE other.table_name (different integer)",
        "INSERT INTO other.table_name VALUES (9)",
        "CREATE TABLE copy_heap (test integer)",
        "CREATE TABLE copy_keyed (id integer PRIMARY KEY, label text, marker integer)",
    ] {
        s.execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    for projection in [
        "t.* FROM public.table_name t",
        "table_name.* FROM table_name",
        "public.table_name.* FROM public.table_name",
    ] {
        assert_eq!(
            query(
                &mut s,
                &format!("SELECT {projection} ORDER BY test"),
                &["test"]
            ),
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)]
            ]
        );
    }
    assert_eq!(
        query(
            &mut s,
            "SELECT t.* FROM public.table_name t WHERE t.test > 0 ORDER BY 1 DESC LIMIT 1 OFFSET 1",
            &["test"]
        ),
        vec![vec![Value::Int(2)]]
    );
    assert_eq!(
        query(
            &mut s,
            "SELECT k.*, 1 AS marker FROM keyed k WHERE k.id=1",
            &["id", "label", "marker"]
        ),
        vec![vec![
            Value::Int(1),
            Value::Text("one".into()),
            Value::Int(1)
        ]]
    );
    assert_eq!(
        query(
            &mut s,
            "SELECT a.*, b.label FROM table_name a JOIN keyed b ON a.test=b.id ORDER BY 1",
            &["test", "label"]
        ),
        vec![
            vec![Value::Int(1), Value::Text("one".into())],
            vec![Value::Int(2), Value::Text("two".into())]
        ]
    );
    assert_eq!(
        query(
            &mut s,
            "SELECT b.*, a.* FROM table_name a LEFT JOIN keyed b ON a.test=b.id WHERE a.test=3",
            &["id", "label", "test"]
        ),
        vec![vec![Value::Null, Value::Null, Value::Int(3)]]
    );
    assert_eq!(
        query(
            &mut s,
            "SELECT other.table_name.* FROM other.table_name",
            &["different"]
        ),
        vec![vec![Value::Int(9)]]
    );
    assert_eq!(
        query(
            &mut s,
            r#"SELECT "T".* FROM table_name AS "T" WHERE "T".test=1"#,
            &["test"]
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        query(
            &mut s,
            "SELECT t.* FROM table_name t WHERE test IN (SELECT k.* FROM table_name k WHERE k.test=1)",
            &["test"]
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        query(
            &mut s,
            "SELECT t.*, count(*) AS n FROM table_name t GROUP BY t.test ORDER BY 1",
            &["test", "n"]
        ),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(3), Value::Int(1)]
        ]
    );
    s.execute(
        "INSERT INTO copy_heap SELECT t.* FROM table_name t ORDER BY 1 LIMIT 1",
        &[],
        None,
    )
    .unwrap();
    assert_eq!(
        query(&mut s, "SELECT c.* FROM copy_heap c", &["test"]),
        vec![vec![Value::Int(1)]]
    );
    let insert = s
        .prepare("INSERT INTO copy_keyed SELECT k.*, $1 FROM table_name a JOIN keyed k ON a.test=k.id ORDER BY 1")
        .unwrap();
    s.execute_prepared(&insert, &[Value::Int(7)], None).unwrap();
    assert_eq!(
        query(
            &mut s,
            "SELECT c.* FROM copy_keyed c WHERE id=1",
            &["id", "label", "marker"]
        ),
        vec![vec![
            Value::Int(1),
            Value::Text("one".into()),
            Value::Int(7)
        ]]
    );
    for sql in [
        "INSERT INTO table_name VALUES (4) RETURNING table_name.*",
        "UPDATE table_name SET test=4 WHERE test=4 RETURNING public.table_name.*",
        "DELETE FROM table_name WHERE test=4 RETURNING table_name.*",
    ] {
        let prepared = s.prepare(sql).unwrap();
        let description = s.describe(&prepared).unwrap();
        assert_eq!(description.columns.len(), 1);
        assert_eq!(description.columns[0].name, "test");
        assert_eq!(description.columns[0].data_type, DataType::Integer);
        let result = s
            .execute_prepared(&prepared, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_eq!(result.columns, ["test"]);
        assert_eq!(result.rows[0].values, vec![Value::Int(4)]);
    }
    for sql in [
        "SELECT missing.* FROM table_name",
        "SELECT missing.*",
        "SELECT table_name.* FROM table_name t",
        "SELECT public.table_name.* FROM public.table_name t",
        "SELECT public.table_name.* FROM public.table_name AS table_name",
        "SELECT other.table_name.* FROM public.table_name",
        "SELECT missing.*, count(*) FROM table_name GROUP BY test",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(SqlError::UnknownTable)),
            "{sql}"
        );
    }
    assert_eq!(
        s.execute("SELECT t.*, count(*) FROM table_name t", &[], None),
        Err(CoreError::Sql(SqlError::GroupingError))
    );
    assert_eq!(
        s.execute("SELECT t.* AS renamed FROM table_name t", &[], None),
        s.execute("SELECT * AS renamed FROM table_name t", &[], None),
    );
    assert_eq!(
        query(&mut s, "SELECT q.* FROM (SELECT test FROM table_name) q ORDER BY test", &["test"]),
        query(&mut s, "SELECT * FROM (SELECT test FROM table_name) q ORDER BY test", &["test"]),
    );
    let expected = s.execute("WITH q AS (SELECT test FROM table_name) SELECT * FROM q ORDER BY test", &[], None).unwrap();
    let qualified = s.execute("WITH q AS (SELECT test FROM table_name) SELECT q.* FROM q ORDER BY test", &[], None).unwrap();
    assert_eq!(qualified, expected);
    assert_eq!(expected.rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
        vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]]);
    drop(s);
    drop(admin);
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn legacy_qualified_wildcard() {
    exercise(Engine::Legacy);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_qualified_wildcard() {
    exercise(Engine::V2);
}
