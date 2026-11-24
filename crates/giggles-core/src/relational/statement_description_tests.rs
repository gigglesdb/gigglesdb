use super::*;
use crate::{Admin, Engine, Limits, SqlSession, sqlstate};

fn database() -> Admin {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let root = std::env::temp_dir().join(format!(
        "statement-description-{:032x}",
        u128::from_be_bytes(nonce)
    ));
    Admin::create_engine(root, Limits::default(), Engine::V2).unwrap()
}

fn execute(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None).unwrap()
}

fn execute_counted(
    s: &mut SqlSession,
    p: &Prepared,
    params: &[Value],
    descriptions: usize,
) -> SqlResult {
    searched::DESCRIBE_CALLS.set(0);
    let result = s.execute_prepared(p, params, None).unwrap();
    assert_eq!(searched::DESCRIBE_CALLS.get(), descriptions);
    result
}

#[test]
fn point_update_returning_describes_once_and_rebinds_after_ddl() {
    let admin = database();
    let mut s = admin.sql_session();
    execute(
        &mut s,
        "CREATE TABLE t(id bigint PRIMARY KEY, n bigint)",
    );
    // Ordinary pages omit non-UNIQUE sidecars. This unrelated name must
    // still allow the preflight description to be reused.
    execute(&mut s, "CREATE INDEX t_n_idx ON t(n)");
    execute(&mut s, "INSERT INTO t VALUES (1, 10)");
    let p = s
        .prepare("UPDATE t SET n = n + 1 WHERE id = $1 RETURNING *")
        .unwrap()
        .with_parameter_types(&[Some(DataType::BigInt)])
        .unwrap();

    let hit = execute_counted(&mut s, &p, &[Value::Int(1)], 1);
    assert_eq!(hit.affected, 1);
    assert_eq!(hit.columns, ["id", "n"]);
    assert_eq!(hit.rows[0].values, [Value::Int(1), Value::Int(11)]);

    let miss = execute_counted(&mut s, &p, &[Value::Int(2)], 1);
    assert_eq!(miss.affected, 0);
    assert!(miss.rows.is_empty());
    assert_eq!(miss.columns, ["id", "n"]);

    let bad = s
        .execute_prepared(&p, &[Value::Bool(true)], None)
        .unwrap_err();
    assert_eq!(sqlstate(&bad), "42804");

    execute(&mut s, "ALTER TABLE t ADD COLUMN extra bigint DEFAULT 7");
    let hit = execute_counted(&mut s, &p, &[Value::Int(1)], 1);
    assert_eq!(hit.columns, ["id", "n", "extra"]);
    assert_eq!(
        hit.rows[0].values,
        [Value::Int(1), Value::Int(12), Value::Int(7)]
    );
}

#[test]
fn cascade_delete_and_optimistic_miss_each_describe_once() {
    let admin = database();
    let mut s = admin.sql_session();
    for sql in [
        "CREATE TABLE parent(id bigint PRIMARY KEY, version bigint)",
        "CREATE TABLE child(id bigint PRIMARY KEY, pid bigint REFERENCES parent(id) ON DELETE CASCADE)",
        "INSERT INTO parent VALUES (1, 0), (2, 1), (3, 0), (4, 0)",
        "INSERT INTO child VALUES (1, 1), (2, 2), (3, 3), (4, 4)",
    ] {
        execute(&mut s, sql);
    }

    let optimistic = s
        .prepare("UPDATE parent SET version = version + 1 WHERE id = $1 AND version = 0")
        .unwrap();
    let miss = execute_counted(&mut s, &optimistic, &[Value::Int(2)], 1);
    assert_eq!(miss, SqlResult::default());

    let delete = s
        .prepare("DELETE FROM parent WHERE id IN (4, 2, 1) RETURNING id, version")
        .unwrap();
    let result = execute_counted(&mut s, &delete, &[], 1);
    assert_eq!(result.affected, 3);
    assert_eq!(result.columns, ["id", "version"]);
    assert_eq!(
        result
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        [
            vec![Value::Int(1), Value::Int(0)],
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(4), Value::Int(0)],
        ]
    );
    let child = execute(&mut s, "SELECT id FROM child");
    assert_eq!(child.rows.len(), 1);
    assert_eq!(child.rows[0].values, [Value::Int(3)]);
}

#[test]
fn searched_update_returning_shares_description_across_pages() {
    let admin = database();
    let mut s = admin.sql_session();
    execute(&mut s, "CREATE TABLE t(id bigint PRIMARY KEY, n bigint)");
    let values = (1..=201).map(|i| format!("({i}, 10)")).collect::<Vec<_>>();
    execute(
        &mut s,
        &format!("INSERT INTO t VALUES {}", values.join(",")),
    );
    let update = s.prepare("UPDATE t SET n = n + 1 RETURNING *").unwrap();
    let result = execute_counted(&mut s, &update, &[], 1);
    assert_eq!(result.affected, 201);
    assert_eq!(result.columns, ["id", "n"]);
    assert_eq!(
        result
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        (1..=201)
            .map(|i| vec![Value::Int(i), Value::Int(11)])
            .collect::<Vec<_>>()
    );
}

#[test]
fn rewritten_selection_has_its_own_description() {
    let admin = database();
    let mut s = admin.sql_session();
    execute(&mut s, "CREATE TABLE t(id bigint PRIMARY KEY, n bigint)");
    execute(&mut s, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let update = s
        .prepare("UPDATE t SET n = 30 WHERE id IN (SELECT id FROM t WHERE n = $1) RETURNING id")
        .unwrap();
    let result = execute_counted(&mut s, &update, &[Value::Int(20)], 2);
    assert_eq!(result.affected, 1);
    assert_eq!(result.rows[0].values, [Value::Int(2)]);
}

#[test]
fn cascade_delete_with_rewritten_selection_uses_two_descriptions() {
    let admin = database();
    let mut s = admin.sql_session();
    for sql in [
        "CREATE TABLE parent(id bigint PRIMARY KEY, n bigint)",
        "CREATE TABLE child(id bigint PRIMARY KEY, pid bigint REFERENCES parent(id) ON DELETE CASCADE)",
        "INSERT INTO parent VALUES (1, 10), (2, 20), (3, 20)",
        "INSERT INTO child VALUES (1, 1), (2, 2), (3, 3)",
    ] {
        execute(&mut s, sql);
    }

    let delete = s
        .prepare("DELETE FROM parent WHERE id IN (SELECT id FROM parent WHERE n = $1) RETURNING id")
        .unwrap();
    let result = execute_counted(&mut s, &delete, &[Value::Int(20)], 2);
    assert_eq!(result.affected, 2);
    assert_eq!(result.columns, ["id"]);
    assert_eq!(result.rows[0].values, [Value::Int(2)]);
    assert_eq!(result.rows[1].values, [Value::Int(3)]);
    let child = execute(&mut s, "SELECT pid FROM child");
    assert_eq!(child.rows.len(), 1);
    assert_eq!(child.rows[0].values, [Value::Int(1)]);
}

#[test]
fn prepared_returning_keeps_date_order_and_parameter_types_after_datestyle_change() {
    use giggles_pg_types::{
        bytea::ByteaOutput,
        temporal::{DateOrder, DateStyle, DateStyleSettings},
    };

    let admin = database();
    let mut s = admin.sql_session();
    for sql in [
        "CREATE TABLE t(id bigint PRIMARY KEY, d date, n bigint)",
        "CREATE TABLE keys(id bigint PRIMARY KEY)",
        "INSERT INTO t VALUES (1, '2020-01-13', 10), (2, '2020-01-14', 20)",
        "INSERT INTO keys VALUES (1), (2)",
    ] {
        execute(&mut s, sql);
    }
    let style = |order| DateStyleSettings {
        style: DateStyle::Iso,
        order,
    };
    for (sql, descriptions, expected_n) in [
        (
            "UPDATE t SET n = n + $1 WHERE d = '13/01/2020' RETURNING id, n",
            1,
            12,
        ),
        (
            "UPDATE t SET n = n + $1 WHERE d = '13/01/2020' AND id IN (SELECT id FROM keys) RETURNING id, n",
            2,
            14,
        ),
    ] {
        s.set_text_settings(style(DateOrder::Dmy), ByteaOutput::default());
        let prepared = s.prepare(sql).unwrap();
        let types = s.describe(&prepared).unwrap().parameters;
        assert_eq!(types, [DataType::BigInt]);

        s.set_text_settings(style(DateOrder::Mdy), ByteaOutput::default());
        assert_eq!(s.describe(&prepared).unwrap().parameters, types);
        let params = [Value::Int(2)];
        let error = s.execute(sql, &params, None).unwrap_err();
        assert_eq!(sqlstate(&error), "22008");
        let result = execute_counted(&mut s, &prepared, &params, descriptions);
        assert_eq!(result.affected, 1);
        assert_eq!(result.rows[0].values, [Value::Int(1), Value::Int(expected_n)]);
    }
}

#[test]
fn update_returning_preserves_constraint_sqlstates() {
    let admin = database();
    let mut s = admin.sql_session();
    execute(
        &mut s,
        "CREATE TABLE t(id bigint PRIMARY KEY, n bigint UNIQUE NOT NULL CHECK (n > 0))",
    );
    execute(&mut s, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let update = s
        .prepare("UPDATE t SET n = $1 WHERE id = 1 RETURNING *")
        .unwrap();
    for (value, expected) in [
        (Value::Int(-1), "23514"),
        (Value::Null, "23502"),
        (Value::Int(20), "23505"),
    ] {
        let error = s.execute_prepared(&update, &[value], None).unwrap_err();
        assert_eq!(sqlstate(&error), expected);
    }
    let rows = execute(&mut s, "SELECT n FROM t ORDER BY id").rows;
    assert_eq!(rows[0].values, [Value::Int(10)]);
    assert_eq!(rows[1].values, [Value::Int(20)]);
}
