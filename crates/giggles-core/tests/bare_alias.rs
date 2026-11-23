mod common;
use giggles_core::{Admin, Engine, Limits, relational::Value};

fn exercise(engine: Engine) {
    let path = common::fresh("bare_alias");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE t (id integer PRIMARY KEY, name text)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO t VALUES (1, 'u.name')", &[], None)
        .unwrap();
    s.execute(
        "CREATE TABLE orders (id integer, user_id integer, total integer)",
        &[],
        None,
    )
    .unwrap();
    s.execute(
        "CREATE TABLE users (id integer, user_id integer)",
        &[],
        None,
    )
    .unwrap();
    s.execute(
        "CREATE TABLE items (order_id integer, id integer)",
        &[],
        None,
    )
    .unwrap();
    // Correct outer binding hits orders 1 and 2 for EXISTS, and 3 and 4 for
    // NOT EXISTS. Binding o.user_id to users.user_id hits none for EXISTS
    // (neither users row has id = user_id), and all four for NOT EXISTS.
    let original = vec![[1, 1, 10], [2, 2, 20], [3, 3, 30], [4, 99, 990]];
    let values = |rows: Vec<[i64; 3]>| {
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .map(giggles_core::relational::Value::Int)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    let run_correlated = |sql: &str| {
        let path = common::fresh("bare_alias_correlated");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut session = admin.sql_session();
        for seed in [
            "CREATE TABLE orders (id integer, user_id integer, total integer)",
            "CREATE TABLE users (id integer, user_id integer)",
            "CREATE TABLE items (order_id integer, id integer)",
            "INSERT INTO orders VALUES (1,1,10),(2,2,20),(3,3,30),(4,99,990)",
            "INSERT INTO users VALUES (1,99),(2,1)",
            "INSERT INTO items VALUES (2,20),(4,40)",
        ] {
            session.execute(seed, &[], None).unwrap();
        }
        let affected = session
            .execute(sql, &[], None)
            .map(|result| result.affected);
        let rows = session
            .execute(
                "SELECT id, user_id, total FROM orders ORDER BY id",
                &[],
                None,
            )
            .unwrap()
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>();
        drop(session);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
        (affected, rows)
    };
    for (aliased, plain, affected, expected, correlated_in) in [
        (
            "DELETE FROM orders o WHERE EXISTS (SELECT 1 FROM users WHERE users.id = o.user_id)",
            "DELETE FROM orders WHERE EXISTS (SELECT 1 FROM users WHERE users.id = orders.user_id)",
            2,
            vec![[3, 3, 30], [4, 99, 990]],
            false,
        ),
        (
            "UPDATE orders o SET total = 0 WHERE EXISTS (SELECT 1 FROM users WHERE users.id = o.user_id)",
            "UPDATE orders SET total = 0 WHERE EXISTS (SELECT 1 FROM users WHERE users.id = orders.user_id)",
            2,
            vec![[1, 1, 0], [2, 2, 0], [3, 3, 30], [4, 99, 990]],
            false,
        ),
        (
            "DELETE FROM orders o WHERE NOT EXISTS (SELECT 1 FROM users WHERE users.id = o.user_id)",
            "DELETE FROM orders WHERE NOT EXISTS (SELECT 1 FROM users WHERE users.id = orders.user_id)",
            2,
            vec![[1, 1, 10], [2, 2, 20]],
            false,
        ),
        (
            "UPDATE orders o SET total = 0 WHERE o.id IN (SELECT order_id FROM items WHERE items.order_id = o.id)",
            "UPDATE orders SET total = 0 WHERE orders.id IN (SELECT order_id FROM items WHERE items.order_id = orders.id)",
            2,
            vec![[1, 1, 10], [2, 2, 0], [3, 3, 30], [4, 99, 0]],
            true,
        ),
    ] {
        let aliased_result = run_correlated(aliased);
        let plain_result = run_correlated(plain);
        assert_eq!(
            aliased_result, plain_result,
            "aliased: {aliased}; plain: {plain}"
        );
        if correlated_in {
            // Correlated IN remains outside the admitted subset for both forms.
            assert_eq!(
                aliased_result.0,
                Err(giggles_core::CoreError::Sql(
                    giggles_core::relational::SqlError::Unsupported,
                )),
                "0A000: {aliased}; {plain}"
            );
            assert_eq!(
                aliased_result.1,
                values(original.clone()),
                "0A000: {aliased}"
            );
        } else {
            assert_eq!(aliased_result.0, Ok(affected), "{aliased}; {plain}");
            assert_eq!(aliased_result.1, values(expected), "{aliased}; {plain}");
        }
    }
    for alias in ["n", "total", "rank", "value", "name", "type"] {
        let result = s
            .execute(&format!("SELECT count(*) {alias} FROM t"), &[], None)
            .unwrap();
        assert_eq!(result.columns[0], alias);
    }
    for sql in ["SELECT 1 year", "SELECT 1 from", "SELECT 1 where"] {
        assert_eq!(
            s.execute(sql, &[], None).unwrap_err(),
            giggles_core::CoreError::Sql(giggles_core::relational::SqlError::Syntax),
            "42601: {sql}"
        );
    }
    for sql in [
        "UPDATE t u SET name = u.name WHERE u.id = 1",
        "UPDATE t AS u SET name = 'u.name' WHERE u.id = 1 RETURNING u.id",
        "UPDATE public.t u SET name = u.name WHERE u.id = 1 RETURNING u.id",
        "UPDATE t u SET name = u.name WHERE u.id IN (SELECT id FROM t)",
    ] {
        s.execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    assert_eq!(
        s.execute("UPDATE t u SET name = t.name", &[], None)
            .unwrap_err(),
        giggles_core::CoreError::Sql(giggles_core::relational::SqlError::UnknownTable)
    );
    for sql in [
        "UPDATE t u SET name = u.name WHERE u.id IN (SELECT id FROM t u)",
        "UPDATE t u SET name = u.name WHERE u.id IN (SELECT id FROM t AS u)",
    ] {
        let result = s
            .execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_eq!(result.affected, 1, "{sql}");
        let rows = s
            .execute("SELECT id, name FROM t", &[], None)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1, "{sql}");
        assert_eq!(
            rows[0].values,
            [Value::Int(1), Value::Text("u.name".into())],
            "{sql}"
        );
    }
    // Roll back the deletion so the remaining alias checks still have a row.
    s.execute("BEGIN", &[], None).unwrap();
    let deleted = s
        .execute(
            "DELETE FROM t u WHERE u.id IN (SELECT id FROM t AS u)",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(deleted.affected, 1);
    assert!(
        s.execute("SELECT id FROM t", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    s.execute("ROLLBACK", &[], None).unwrap();
    for sql in [
        "SELECT sum(id) total FROM t",
        "SELECT id, count(*) n FROM t GROUP BY id",
    ] {
        s.execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    s.execute("DELETE FROM t u WHERE u.id = 1 RETURNING u.id", &[], None)
        .unwrap();
}

#[test]
fn legacy() {
    exercise(Engine::Legacy);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn rocksdb() {
    exercise(Engine::V2);
}
