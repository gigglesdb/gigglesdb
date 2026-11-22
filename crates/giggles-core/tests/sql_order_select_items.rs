mod common;
use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{StatementClock, Value},
    sqlstate,
};

const CLOCK: StatementClock = StatementClock {
    unavailable: None,
    transaction_us: 100,
    statement_us: 100,
    zone: None,
    wall: None,
};

fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    session
        .execute_at(sql, &[], None, &CLOCK)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn ints(expected: &[&[i64]]) -> Vec<Vec<Value>> {
    expected
        .iter()
        .map(|row| row.iter().copied().map(Value::Int).collect())
        .collect()
}

fn assert_state(session: &mut SqlSession, sql: &str, expected: &str) {
    let error = session.execute_at(sql, &[], None, &CLOCK).unwrap_err();
    assert_eq!(sqlstate(&error), expected, "{sql}: {error:?}");
}

fn exercise(engine: Engine) {
    let admin = Admin::create_engine(
        common::fresh("order-select-items"),
        Limits::default(),
        engine,
    )
    .unwrap();
    let mut session = admin.sql_session();
    rows(&mut session, "CREATE TABLE t(a int, b int)");
    rows(&mut session, "INSERT INTO t VALUES (1,20),(2,10)");
    for index in 1..=19 {
        rows(&mut session, &format!("CREATE SEQUENCE c{index}"));
    }
    let oracle: &[(&str, &[&[i64]], &str, i64)] = &[
        (
            "SELECT nextval('c1') AS n FROM t ORDER BY 1",
            &[&[1], &[2]],
            "nextval('c1')",
            3,
        ),
        (
            "SELECT nextval('c2') AS n, a FROM t ORDER BY 1 DESC",
            &[&[2, 2], &[1, 1]],
            "nextval('c2')",
            3,
        ),
        (
            "SELECT nextval('c3') AS n FROM t ORDER BY n",
            &[&[1], &[2]],
            "currval('c3')",
            2,
        ),
        (
            "SELECT nextval('c4') AS n, a FROM t ORDER BY n DESC",
            &[&[2, 2], &[1, 1]],
            "currval('c4')",
            2,
        ),
        (
            "SELECT a, nextval('c5') AS n FROM t ORDER BY 2 DESC, 1",
            &[&[2, 2], &[1, 1]],
            "currval('c5')",
            2,
        ),
        (
            "SELECT DISTINCT nextval('c6') AS n FROM t ORDER BY 1 DESC",
            &[&[2], &[1]],
            "currval('c6')",
            2,
        ),
        (
            "SELECT nextval('c7') AS n, count(*) FROM t GROUP BY 1 ORDER BY 1",
            &[&[1, 1], &[2, 1]],
            "currval('c7')",
            2,
        ),
        (
            "SELECT nextval('c8') + 0 AS n, a FROM t ORDER BY 1 DESC LIMIT 1",
            &[&[2, 2]],
            "currval('c8')",
            2,
        ),
        (
            "SELECT a + 1 AS x, nextval('c9') AS n FROM t ORDER BY x DESC",
            &[&[3, 1], &[2, 2]],
            "currval('c9')",
            2,
        ),
        (
            "SELECT nextval('c12') AS n, a FROM t ORDER BY 1 DESC OFFSET 1",
            &[&[1, 1]],
            "currval('c12')",
            2,
        ),
        (
            "SELECT nextval('c14') AS a FROM t ORDER BY a",
            &[&[1], &[2]],
            "currval('c14')",
            2,
        ),
        (
            "SELECT nextval('c15') AS n, a FROM t ORDER BY n DESC, 1",
            &[&[2, 2], &[1, 1]],
            "currval('c15')",
            2,
        ),
        (
            "SELECT nextval('c16') AS n, nextval('c16') AS m FROM t ORDER BY 2 DESC, 1",
            &[&[3, 4], &[1, 2]],
            "currval('c16')",
            4,
        ),
        (
            "SELECT nextval('c17'), t.a FROM t JOIN t u ON t.a = u.a ORDER BY nextval DESC",
            &[&[2, 2], &[1, 1]],
            "currval('c17')",
            2,
        ),
        (
            "SELECT nextval('c18') AS n, count(*) FROM t GROUP BY a ORDER BY n DESC",
            &[&[2, 1], &[1, 1]],
            "currval('c18')",
            2,
        ),
        (
            "SELECT nextval('c19') AS n ORDER BY n",
            &[&[1]],
            "currval('c19')",
            1,
        ),
    ];
    for &(sql, expected, follow_up, value) in oracle {
        assert_eq!(rows(&mut session, sql), ints(expected), "{sql}");
        assert_eq!(
            rows(&mut session, &format!("SELECT {follow_up}")),
            ints(&[&[value]]),
            "{follow_up}"
        );
    }

    assert_state(
        &mut session,
        "SELECT nextval('c10') AS n FROM t ORDER BY n + 0",
        "42703",
    );
    assert_state(&mut session, "SELECT currval('c10')", "55000");
    // PostgreSQL uses 42P10. Keep the existing GigglesDB 42703 here.
    assert_state(
        &mut session,
        "SELECT *, nextval('c11') AS n FROM t ORDER BY 4 DESC",
        "42703",
    );
    assert_state(&mut session, "SELECT currval('c11')", "55000");

    let sql = "SELECT nextval('c13') AS n, a FROM t WHERE a <= $1 ORDER BY 1 DESC";
    // The SQL PREPARE and EXECUTE commands are an existing unsupported surface.
    assert_state(&mut session, &format!("PREPARE p(int) AS {sql}"), "0A000");
    assert_state(&mut session, "EXECUTE p(5)", "0A000");
    let prepared = session.prepare(sql).unwrap();
    assert_state(&mut session, "SELECT currval('c13')", "55000");
    let result = session
        .execute_prepared_at(&prepared, &[Value::Int(5)], None, &CLOCK)
        .unwrap();
    assert_eq!(
        result
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        ints(&[&[2, 2], &[1, 1]])
    );
    assert_eq!(rows(&mut session, "SELECT currval('c13')"), ints(&[&[2]]));

    for order in ["1", "x"] {
        assert_eq!(
            rows(
                &mut session,
                &format!("SELECT b + 1 AS x FROM t ORDER BY {order}")
            ),
            ints(&[&[11], &[21]])
        );
    }
    rows(&mut session, "CREATE TABLE many(a int PRIMARY KEY)");
    let values = (1..=50)
        .map(|n| format!("({n})"))
        .collect::<Vec<_>>()
        .join(",");
    rows(&mut session, &format!("INSERT INTO many VALUES {values}"));
    assert_eq!(
        rows(
            &mut session,
            "SELECT a + 1 FROM many ORDER BY 1 DESC LIMIT 3"
        ),
        ints(&[&[51], &[50], &[49]])
    );
    rows(&mut session, "CREATE SEQUENCE shadow");
    assert_eq!(
        rows(
            &mut session,
            "SELECT nextval('shadow')::int AS a, many.a AS original FROM many WHERE many.a <= 2 ORDER BY a DESC"
        ),
        ints(&[&[2, 2], &[1, 1]])
    );
    assert_eq!(
        rows(&mut session, "SELECT currval('shadow')"),
        ints(&[&[2]])
    );
}

fn review_regressions(engine: Engine) {
    let path = common::fresh("order-select-review");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut session = admin.sql_session();
    rows(&mut session, "CREATE TABLE gd92_cases(a int PRIMARY KEY, b int)");
    rows(&mut session, "INSERT INTO gd92_cases VALUES (1,20),(2,10),(3,10)");

    for (sequence, query, expected, follow_up, value) in [
        (
            "gd92_same_expr",
            "SELECT a, nextval('gd92_same_expr') AS n FROM gd92_cases ORDER BY nextval('gd92_same_expr') DESC, n",
            ints(&[&[3, 3], &[2, 2], &[1, 1]]),
            "currval",
            3,
        ),
        (
            "gd92_expr_only",
            "SELECT a, nextval('gd92_expr_only') AS n FROM gd92_cases ORDER BY nextval('gd92_expr_only') DESC",
            ints(&[&[3, 3], &[2, 2], &[1, 1]]),
            "currval",
            3,
        ),
        (
            "gd92_dist_expr",
            "SELECT DISTINCT a, nextval('gd92_dist_expr') AS n FROM gd92_cases ORDER BY nextval('gd92_dist_expr') DESC, n",
            ints(&[&[3, 3], &[2, 2], &[1, 1]]),
            "currval",
            3,
        ),
        (
            "gd92_group_expr",
            "SELECT b, nextval('gd92_group_expr') + b AS n, count(*) FROM gd92_cases WHERE b = 10 GROUP BY b ORDER BY nextval('gd92_group_expr') + b DESC, n",
            ints(&[&[10, 11, 2]]),
            "currval",
            1,
        ),
        (
            "gd92_independent",
            "SELECT a FROM gd92_cases WHERE a <= 2 ORDER BY nextval('gd92_independent')",
            ints(&[&[1], &[2]]),
            "nextval",
            3,
        ),
        (
            "gd92_dfa",
            "SELECT DISTINCT nextval('gd92_dfa') AS n ORDER BY n",
            ints(&[&[1]]),
            "currval",
            1,
        ),
        (
            "gd92_dfo",
            "SELECT DISTINCT nextval('gd92_dfo') AS n ORDER BY 1",
            ints(&[&[1]]),
            "currval",
            1,
        ),
        (
            "gd92_dfe",
            "SELECT DISTINCT nextval('gd92_dfe') AS n ORDER BY nextval('gd92_dfe')",
            ints(&[&[1]]),
            "currval",
            1,
        ),
    ] {
        rows(&mut session, &format!("CREATE SEQUENCE {sequence}"));
        assert_eq!(rows(&mut session, query), expected, "{query}");
        let follow_up = format!("SELECT {follow_up}('{sequence}')");
        assert_eq!(rows(&mut session, &follow_up), ints(&[&[value]]), "{follow_up}");
    }

    for query in [
        "SELECT a AS n, b AS n FROM gd92_cases ORDER BY n",
        "SELECT b AS n, count(*) AS n FROM gd92_cases GROUP BY b ORDER BY n",
        "SELECT DISTINCT a AS n, b AS n FROM gd92_cases ORDER BY n",
    ] {
        assert_state(&mut session, query, "42702");
        assert_eq!(
            giggles_core::relational::take_sql_error_message().as_deref(),
            Some(r#"ORDER BY "n" is ambiguous"#),
            "{query}"
        );
    }

    for query in [
        "SELECT a AS n, b AS n FROM gd92_cases",
        "SELECT DISTINCT a AS n, b AS n FROM gd92_cases",
        "SELECT DISTINCT a AS n, b AS n FROM gd92_cases ORDER BY 1",
        "WITH d AS (SELECT a AS n, b AS n FROM gd92_cases) SELECT * FROM d ORDER BY 1",
        "SELECT * FROM (SELECT a AS n, b AS n FROM gd92_cases) d ORDER BY 1",
    ] {
        let result = session.execute_at(query, &[], None, &CLOCK)
            .unwrap_or_else(|error| panic!("{query}: {error:?}"));
        assert_eq!(result.columns, ["n", "n"], "{query}");
        let mut values: Vec<_> = result.rows.into_iter().map(|row| row.values).collect();
        if !query.ends_with("ORDER BY 1") {
            values.sort_by_key(|row| match row[0] {
                Value::Int(a) => a,
                _ => panic!("expected an integer"),
            });
        }
        assert_eq!(values, ints(&[&[1, 20], &[2, 10], &[3, 10]]), "{query}");
    }
    for query in [
        "WITH d AS (SELECT a AS n, b AS n FROM gd92_cases) SELECT * FROM d ORDER BY 2, 1",
        "SELECT * FROM (SELECT a AS n, b AS n FROM gd92_cases) d ORDER BY 2, 1",
    ] {
        assert_eq!(
            rows(&mut session, query),
            ints(&[&[2, 10], &[3, 10], &[1, 20]]),
            "{query}"
        );
    }
    assert_eq!(
        rows(&mut session, "SELECT a AS n, a AS n FROM gd92_cases ORDER BY n DESC"),
        ints(&[&[3, 3], &[2, 2], &[1, 1]])
    );

    assert_state(
        &mut session,
        "CREATE TABLE gd92_dup_ctas AS SELECT a AS n, b AS n FROM gd92_cases",
        "42701",
    );
    // PG 18.6: an existing destination wins over duplicate output names:
    // IF NOT EXISTS skips (NOTICE 42P07), plain CTAS fails 42P07.
    rows(&mut session, "CREATE TABLE gd92_ctas_dst (a int PRIMARY KEY, b int)");
    rows(
        &mut session,
        "CREATE TABLE IF NOT EXISTS gd92_ctas_dst AS SELECT a, a FROM gd92_cases",
    );
    assert_state(
        &mut session,
        "CREATE TABLE gd92_ctas_dst AS SELECT a, a FROM gd92_cases",
        "42P07",
    );
    assert_eq!(rows(&mut session, "SELECT count(*) FROM gd92_ctas_dst"), ints(&[&[0]]));
    if engine == Engine::V2 {
        assert_state(
            &mut session,
            "CREATE VIEW gd92_dup_view AS SELECT a AS n, b AS n FROM gd92_cases",
            "42701",
        );
        rows(
            &mut session,
            "CREATE VIEW gd92_renamed_view (x,y) AS SELECT a AS n, b AS n FROM gd92_cases",
        );
        assert_eq!(
            rows(&mut session, "SELECT * FROM gd92_renamed_view ORDER BY 1"),
            ints(&[&[1, 20], &[2, 10], &[3, 10]])
        );
    }
    for query in [
        "WITH d AS (SELECT a AS n, b AS n FROM gd92_cases) SELECT d.n FROM d",
        "SELECT d.n FROM (SELECT a AS n, b AS n FROM gd92_cases) d",
    ] {
        assert_state(&mut session, query, "42702");
    }
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_order_select_items() {
    exercise(Engine::Legacy);
    review_regressions(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_order_select_items() {
    exercise(Engine::V2);
    review_regressions(Engine::V2);
}
