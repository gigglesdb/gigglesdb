use giggles_core::{
    Admin, CoreError, Limits,
    relational::{SqlError, Value},
};

#[test]
fn trailing_defaults_across_insert_paths() {
    let path = std::env::temp_dir().join(format!("trailing-columns-{}", std::process::id()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session.execute("CREATE TABLE t (id INT PRIMARY KEY, txt TEXT, n INT DEFAULT 42, flag BOOLEAN DEFAULT true, missing TEXT)", &[], None).unwrap();
    for sql in [
        "INSERT INTO t VALUES (1, 'literal')",
        "INSERT INTO t VALUES (2, lower('EXPRESSION'))",
        "INSERT INTO t VALUES (3, 'multi'), (4, 'multi')",
        "INSERT INTO t SELECT 5, 'select'",
        "INSERT INTO t SELECT 10, 'returning' RETURNING id, txt",
        "INSERT INTO t VALUES (6, 'default', DEFAULT)",
        "INSERT INTO t VALUES (8::int, 'cast'::text), (9::int, 'cast'::text)",
    ] {
        session
            .execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let prepared = session
        .prepare("INSERT INTO t VALUES ($1, lower($2))")
        .unwrap();
    session
        .execute_prepared(
            &prepared,
            &[Value::Int(7), Value::Text("PARAM".into())],
            None,
        )
        .unwrap();
    let rows = session
        .execute("SELECT id, n, flag, missing FROM t ORDER BY id", &[], None)
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 10);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(
            row.values,
            vec![
                Value::Int(i as i64 + 1),
                Value::Int(42),
                Value::Bool(true),
                Value::Null
            ]
        );
    }
    for sql in [
        "INSERT INTO t VALUES (20, 'x', 1, true, NULL, 6)",
        "INSERT INTO t VALUES (20, lower('X'), 1, true, NULL, 6)",
        "INSERT INTO t VALUES (20, 'x', 1, true, NULL, 6), (11, 'y', 2, false, NULL, 7)",
        "INSERT INTO t SELECT 20, 'x', 1, true, NULL, 6",
        "INSERT INTO t VALUES (20, 'x'), (11, 'y', 1)",
    ] {
        assert_eq!(
            session.execute(sql, &[], None),
            Err(CoreError::Sql(SqlError::Syntax)),
            "{sql}"
        );
    }
    session
        .execute("CREATE SEQUENCE trailing_seq", &[], None)
        .unwrap();
    session
        .execute(
            "CREATE TABLE seq_t (id INT PRIMARY KEY, n BIGINT DEFAULT nextval('trailing_seq'))",
            &[],
            None,
        )
        .unwrap();
    let clock = giggles_core::relational::StatementClock {
        unavailable: None, transaction_us: 100, statement_us: 100,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)), wall: None,
    };
    for sql in [
        "INSERT INTO seq_t VALUES (1)",
        "INSERT INTO seq_t VALUES (abs(2))",
        "INSERT INTO seq_t VALUES (3), (4)",
        "INSERT INTO seq_t SELECT 5",
    ] {
        session
            .execute_at(sql, &[], None, &clock)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let rows = session
        .execute("SELECT n FROM seq_t ORDER BY id", &[], None)
        .unwrap()
        .rows;
    assert_eq!(
        rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
        (1..=5).map(|i| vec![Value::Int(i)]).collect::<Vec<_>>()
    );
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn many_equality_join_sources() {
    let path = std::env::temp_dir().join(format!("many-joins-{}", std::process::id()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t (id INT PRIMARY KEY)", &[], None)
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1), (2), (3)", &[], None)
        .unwrap();
    for count in [9, 12, 20] {
        let mut sql = String::from("SELECT a0.id FROM t a0");
        for i in 1..count {
            sql.push_str(&format!(" JOIN t a{i} ON a{}.id = a{i}.id", i - 1));
        }
        sql.push_str(" ORDER BY a0.id");
        let start = std::time::Instant::now();
        let rows = session.execute(&sql, &[], None).unwrap().rows;
        assert_eq!(
            rows.iter()
                .map(|row| row.values.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)]
            ]
        );
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
