use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError as E, SqlResult, Value as V},
};
use std::time::{SystemTime, UNIX_EPOCH};
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute_at(
        sql,
        &[],
        None,
        &giggles_core::relational::StatementClock {
            unavailable: None,
            transaction_us: 100,
            statement_us: 100,
            zone: None,
            wall: None,
        },
    )
    .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn values(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    run(s, sql).rows.into_iter().map(|r| r.values).collect()
}
fn err(s: &mut SqlSession, sql: &str, e: E) {
    assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(e)), "{sql}");
}
#[test]
fn unique_updates_follow_current_rows_and_survive_reopen() {
    let path = std::env::temp_dir().join(format!(
        "sql-unique-per-row-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for (name, definition, index) in [
        ("t", "id INT PRIMARY KEY,u INT UNIQUE", ""),
        (
            "ci",
            "id INT PRIMARY KEY,u INT",
            "CREATE UNIQUE INDEX ci_u ON ci(u)",
        ),
    ] {
        run(&mut s, &format!("CREATE TABLE {name}({definition})"));
        if !index.is_empty() {
            run(&mut s, index);
        }
        run(
            &mut s,
            &format!("INSERT INTO {name} VALUES(1,1),(2,2),(3,3)"),
        );
        let before = values(&mut s, &format!("SELECT * FROM {name} ORDER BY id"));
        for update in ["u=u+1", "u=4-u", "u=3"] {
            err(
                &mut s,
                &format!("UPDATE {name} SET {update} RETURNING id,u"),
                E::Duplicate,
            );
            assert_eq!(
                values(&mut s, &format!("SELECT * FROM {name} ORDER BY id")),
                before
            );
        }
        let returned = run(
            &mut s,
            &format!("UPDATE {name} SET u=u-1 WHERE id>=1 RETURNING id,u"),
        );
        assert_eq!(
            returned
                .rows
                .iter()
                .map(|r| r.values.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![V::Int(1), V::Int(0)],
                vec![V::Int(2), V::Int(1)],
                vec![V::Int(3), V::Int(2)]
            ]
        );
        run(&mut s, &format!("UPDATE {name} SET u=u+10"));
        err(
            &mut s,
            &format!("UPDATE {name} SET u=12 WHERE id=1 RETURNING id,u"),
            E::Duplicate,
        );
        run(
            &mut s,
            &format!("UPDATE {name} SET u=NULL WHERE id=1 RETURNING id,u"),
        );
        run(&mut s, &format!("UPDATE {name} SET u=NULL WHERE id=2"));
        run(&mut s, "BEGIN");
        run(&mut s, &format!("UPDATE {name} SET u=100 WHERE id=3"));
        run(&mut s, "ROLLBACK");
        assert_eq!(
            values(&mut s, &format!("SELECT u FROM {name} ORDER BY id")),
            vec![vec![V::Null], vec![V::Null], vec![V::Int(12)]]
        );
    }
    run(
        &mut s,
        "CREATE TABLE m(id INT PRIMARY KEY,a INT,b INT,UNIQUE(a,b))",
    );
    run(&mut s, "INSERT INTO m VALUES(1,1,1),(2,1,2)");
    err(&mut s, "UPDATE m SET b=b+1 WHERE a=1", E::Duplicate);
    err(&mut s, "UPDATE m SET b=b-1 WHERE a=1 AND b=2", E::Duplicate);
    run(&mut s, "UPDATE m SET b=9 WHERE id=1");
    assert_eq!(
        values(&mut s, "SELECT b FROM m ORDER BY id"),
        vec![vec![V::Int(9)], vec![V::Int(2)]]
    );
    run(&mut s, "UPDATE m SET b=NULL");
    run(&mut s, "CREATE TABLE ins(id INT PRIMARY KEY,u INT UNIQUE)");
    err(&mut s, "INSERT INTO ins VALUES(1,5),(2,5)", E::Duplicate);
    assert_eq!(
        values(&mut s, "SELECT count(*) FROM ins"),
        vec![vec![V::Int(0)]]
    );
    // Conflict-assignment arithmetic is absent; identity assignment preserves
    // the original oracle INSERT case, whose conflict action is never entered.
    run(
        &mut s,
        "INSERT INTO ins VALUES(1,5),(2,6) ON CONFLICT(u) DO UPDATE SET u=excluded.u",
    );
    run(&mut s, "CREATE TABLE oc(id INT PRIMARY KEY,u INT UNIQUE)");
    run(&mut s, "INSERT INTO oc VALUES(1,1),(2,2)");
    for sql in [
        "INSERT INTO oc VALUES(1,10) ON CONFLICT(id) DO UPDATE SET u=2 RETURNING id,u",
        "INSERT INTO oc VALUES(1,2),(2,12) ON CONFLICT(id) DO UPDATE SET u=excluded.u RETURNING id,u",
    ] {
        err(&mut s, sql, E::Duplicate);
        assert_eq!(
            values(&mut s, "SELECT * FROM oc ORDER BY id"),
            vec![vec![V::Int(1), V::Int(1)], vec![V::Int(2), V::Int(2)]]
        );
    }
    let result = run(
        &mut s,
        "INSERT INTO oc VALUES(1,10),(2,20) ON CONFLICT(id) DO UPDATE SET u=excluded.u RETURNING id,u",
    );
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        vec![vec![V::Int(1), V::Int(10)], vec![V::Int(2), V::Int(20)]]
    );
    run(&mut s, "CREATE TABLE exact_ci(id INT PRIMARY KEY,u INT)");
    run(&mut s, "CREATE UNIQUE INDEX exact_ci_u ON exact_ci(u)");
    run(&mut s, "INSERT INTO exact_ci VALUES(1,1),(2,2)");
    let old = run(&mut s, "SELECT * FROM exact_ci ORDER BY id").rows;
    for sql in [
        "UPDATE exact_ci SET u=u+1",
        "UPDATE exact_ci SET u=3-u",
        "UPDATE exact_ci SET u=99",
    ] {
        err(&mut s, sql, E::Duplicate);
        assert_eq!(run(&mut s, "SELECT * FROM exact_ci ORDER BY id").rows, old);
    }
    assert_eq!(
        run(&mut s, "UPDATE exact_ci SET u=u RETURNING id,u").affected,
        2
    );
    for (name, columns, index) in [
        ("heap_u", "u INT UNIQUE", ""),
        (
            "composite_index",
            "u INT,v INT",
            "CREATE UNIQUE INDEX composite_u ON composite_index(u,v)",
        ),
    ] {
        run(&mut s, &format!("CREATE TABLE {name}({columns})"));
        if !index.is_empty() {
            run(&mut s, index);
        }
        let tuples = if index.is_empty() {
            "(1),(2)"
        } else {
            "(1,1),(2,1)"
        };
        run(&mut s, &format!("INSERT INTO {name} VALUES{tuples}"));
        err(&mut s, &format!("UPDATE {name} SET u=u+1"), E::Duplicate);
        assert_eq!(
            run(&mut s, &format!("UPDATE {name} SET u=u-1 RETURNING u")).affected,
            2
        );
        run(&mut s, &format!("UPDATE {name} SET u=NULL"));
    }
    // Reopen after both failed and successful statements, including indexed lookups.
    drop(s);
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    assert_eq!(
        values(&mut s, "SELECT id FROM t WHERE u=12"),
        vec![vec![V::Int(3)]]
    );
    assert_eq!(
        values(&mut s, "SELECT id FROM ci WHERE u=12"),
        vec![vec![V::Int(3)]]
    );
    assert_eq!(
        values(&mut s, "SELECT u FROM oc ORDER BY id"),
        vec![vec![V::Int(10)], vec![V::Int(20)]]
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
