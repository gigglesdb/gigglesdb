mod common;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, Value},
};

fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn text(s: &str) -> Value {
    Value::Text(s.into())
}

#[test]
fn enum_declaration_order() {
    let path = common::fresh("pgparity-enum");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TYPE e AS ENUM ('z','a','m')");
    run(&mut s, "CREATE TABLE t(id int PRIMARY KEY, v e, w e)");
    run(
        &mut s,
        "INSERT INTO t VALUES (1,'z','m'),(2,'a','a'),(3,'m','z'),(4,NULL,'a')",
    );
    let rows = run(
        &mut s,
        "SELECT v < 'a', v > 'a', v <= 'a', v >= 'a', v = 'a', v <> 'a', v < w, v BETWEEN 'z' AND 'a' FROM t ORDER BY id",
    );
    let expected: Vec<Vec<Value>> = [
        vec![true, false, true, false, false, true, true, true],
        vec![false, false, true, true, true, false, false, true],
        vec![false, true, false, true, false, true, false, false],
    ]
    .into_iter()
    .map(|r| r.into_iter().map(Value::Bool).collect())
    .chain([vec![Value::Null; 8]])
    .collect();
    assert_eq!(rows, expected);
    assert_eq!(
        run(&mut s, "SELECT v FROM t ORDER BY v"),
        vec![
            vec![text("z")],
            vec![text("a")],
            vec![text("m")],
            vec![Value::Null]
        ]
    );
    assert_eq!(
        run(&mut s, "SELECT v FROM t ORDER BY v DESC NULLS LAST"),
        vec![
            vec![text("m")],
            vec![text("a")],
            vec![text("z")],
            vec![Value::Null]
        ]
    );
    assert_eq!(
        run(&mut s, "SELECT min(v),max(v),min(w),max(w) FROM t"),
        vec![vec![text("z"), text("m"), text("z"), text("m")]]
    );
    assert_eq!(
        run(&mut s, "SELECT greatest(v,w),least(v,w) FROM t ORDER BY id"),
        vec![
            vec![text("m"), text("z")],
            vec![text("a"), text("a")],
            vec![text("m"), text("z")],
            vec![text("a"), text("a")]
        ]
    );
    assert_eq!(
        run(&mut s, "SELECT id FROM t WHERE v > 'z' ORDER BY id"),
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
    assert_eq!(
        s.execute("SELECT id FROM t WHERE v < 'q'", &[], None),
        Err(CoreError::Sql(SqlError::InvalidTextRepresentation))
    );
    assert_eq!(
        giggles_core::relational::take_sql_error_message().as_deref(),
        Some("invalid input value for enum e: \"q\"")
    );
    assert_eq!(
        run(&mut s, "SELECT max(v::text) FROM t"),
        vec![vec![text("z")]]
    );
    run(&mut s, "CREATE INDEX enum_v ON t(v)");
    assert_eq!(run(&mut s, "SELECT v FROM t ORDER BY v"), vec![vec![text("z")],vec![text("a")],vec![text("m")],vec![Value::Null]]);
    assert_eq!(run(&mut s, "SELECT id FROM t WHERE v > 'z' ORDER BY id"), vec![vec![Value::Int(2)],vec![Value::Int(3)]]);
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn uuid_aggregate_resolution_even_when_empty() {
    let path = common::fresh("pgparity-uuid");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TABLE t(id int PRIMARY KEY, v uuid)");
    for populated in [false, true] {
        if populated {
            run(
                &mut s,
                "INSERT INTO t VALUES (1,'ffffffff-ffff-ffff-ffff-ffffffffffff'),(2,'00000000-0000-0000-0000-000000000000')",
            );
        }
        for name in ["min", "max"] {
            let sql = format!("SELECT {name}(v) FROM t");
            let prepared = s.prepare(&sql).unwrap();
            assert!(matches!(
                s.describe(&prepared),
                Err(CoreError::Sql(SqlError::UndefinedFunction))
            ));
            assert_eq!(
                giggles_core::relational::take_sql_error_message(),
                Some(format!("function {name}(uuid) does not exist"))
            );
            assert_eq!(
                s.execute(&sql, &[], None),
                Err(CoreError::Sql(SqlError::UndefinedFunction))
            );
        }
    }
    assert_eq!(
        run(&mut s, "SELECT id FROM t ORDER BY v"),
        vec![vec![Value::Int(2)], vec![Value::Int(1)]]
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn cast_array_column_names() {
    let path = common::fresh("pgparity-cast-names");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TABLE t(id int PRIMARY KEY)");
    run(&mut s, "INSERT INTO t VALUES (1)");
    for (expr, name) in [
        ("'{1,2}'::integer[]", "int4"),
        ("'{a}'::text[]", "text"),
        ("'{1}'::int8[]", "int8"),
        ("'{1}'::int[][]", "int4"),
        ("1::integer", "int4"),
        ("1::bigint", "int8"),
        ("1::numeric", "numeric"),
    ] {
        for suffix in ["", " FROM t"] {
            let r = s
                .execute(&format!("SELECT {expr}{suffix}"), &[], None)
                .unwrap();
            assert_eq!(r.columns[0], name, "{expr}{suffix}");
        }
    }
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
